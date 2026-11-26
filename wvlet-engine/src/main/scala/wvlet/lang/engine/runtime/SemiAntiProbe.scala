/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine.runtime

import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.VExprs
import wvlet.uni.cloud.parquet.ParquetFilter

import scala.collection.mutable

/**
  * An EXISTS and a NOT EXISTS over the same table on the same key, checked in one probe pass
  * (`semi_anti_one_probe`). TPC-H q21 keeps the late line items `l1` of orders where another
  * supplier has a line (EXISTS over `l2`) but no other supplier has a late one (NOT EXISTS over
  * `l3`): after decorrelation, a semi join of B with T inside an anti join with `T where p`, both
  * building on B and probing T on the same key. Since each reads T in its own pass, q21 read
  * lineitem three times.
  *
  * `Anti(Semi(B, T, k, r1), σp(T), k, r2)` keeps the rows b of B with a row t of T such that k(b) =
  * k(t) and r1(b, t), and with no row t of T such that k(b) = k(t), p(t), and r2(b, t). So one
  * RIGHT SEMI join building on B probes T once: each probe row with an equal key marks b matched
  * when r1 holds and excluded when p and r2 hold, and the join emits the matched rows that are not
  * excluded ([[wvlet.lang.engine.exec.HashJoinOp]]'s `exclude`). The semi join may also be the
  * outer one, `Semi(Anti(B, σp(T)), T)`, with the same result. Each side may filter T, and its
  * filter joins its condition. The conditions must be deterministic, since the fused join evaluates
  * them once per pair of rows instead of once per side
  */
object SemiAntiProbe:
  import RuntimePlan.*

  /**
    * One probe of `probe` (a scan of T) for the build `build`: keys `buildKeys` over build =
    * `probeKeys` over probe, `matches` the semi join's condition, `exclude` the anti join's
    * condition, both over build ++ probe
    */
  case class Fused(
      build: RuntimePlan,
      probe: Scan,
      buildKeys: IndexedSeq[BExpr],
      probeKeys: IndexedSeq[BExpr],
      matches: Option[BExpr],
      exclude: BExpr
  ):
    /** The semi join the fused join is, without its exclusion */
    def semiJoin: Join = Join(build, probe, JoinKind.Semi, buildKeys, probeKeys, matches, true)

  /** A probe side: the scan, its conjuncts, and each output column's id to its scan column id */
  private case class Side(scan: Scan, conds: List[BExpr], ids: Map[Int, Int])

  /**
    * The fused probe of `j`, when it is a semi and an anti join over the same table on equal keys
    */
  def of(j: Join): Option[Fused] =
    j match
      case Join(inner: Join, outerProbe, outerKind, outerLk, outerRk, outerResidual, true)
          if inner.buildLeft && outerLk.nonEmpty && kinds(inner.kind, outerKind) =>
        for
          a <- side(inner.right)
          b <- side(outerProbe)
          f <- fuse(inner, a, outerKind, b, outerLk, outerRk, outerResidual)
        yield f
      case _ =>
        None

  private def kinds(inner: JoinKind, outer: JoinKind): Boolean =
    (inner == JoinKind.Semi && outer == JoinKind.Anti) ||
      (inner == JoinKind.Anti && outer == JoinKind.Semi)

  private def fuse(
      inner: Join,
      innerSide: Side,
      outerKind: JoinKind,
      outerSide: Side,
      outerLk: IndexedSeq[BExpr],
      outerRk: IndexedSeq[BExpr],
      outerResidual: Option[BExpr]
  ): Option[Fused] =
    val (s1, s2) = (innerSide.scan, outerSide.scan)
    if s1.source.key != s2.source.key || inner.leftKeys.isEmpty then
      return None
    // A scan's row-group filter only skips data; the filter over it stays. One probe reads the row
    // groups either scan reads
    val rowGroups =
      (s1.filter, s2.filter) match
        case (Some(f1), Some(f2)) if f1 != f2 =>
          Some(ParquetFilter.Or(f1, f2))
        case (f1, f2) =>
          f1.flatMap(_ => f2)
    // The probe reads the columns of both scans: those of s2, then those only s1 reads, each with
    // its scan's id
    def positions(s: Scan): Map[Int, Int] =
      s.columns.indices.map(i => s.sourcePosition(i) -> s.columns(i).id.value).toMap
    val p2          = positions(s2)
    val only1       = s1.columns.indices.filterNot(i => p2.contains(s1.sourcePosition(i)))
    val probe: Scan = Scan(
      s2.source,
      s2.columns ++ only1.map(s1.columns),
      Some(s2.columns.indices.map(s2.sourcePosition) ++ only1.map(s1.sourcePosition)),
      rowGroups
    )
    val toProbe = positions(probe)
    // Each side's output column ids to the probe scan's column ids
    def onProbe(side: Side): Map[Int, Int] =
      val scanToProbe =
        side
          .scan
          .columns
          .indices
          .map(i => side.scan.columns(i).id.value -> toProbe(side.scan.sourcePosition(i)))
          .toMap
      side.ids.view.mapValues(scanToProbe).toMap ++ scanToProbe
    val (m1, m2) = (onProbe(innerSide), onProbe(outerSide))
    val build    = inner.left
    // The outer join's left input is the inner join, whose output is the build's columns
    val innerRk  = inner.rightKeys.map(rename(_, m1))
    val outerRk2 = outerRk.map(rename(_, m2))
    val sameKeys =
      inner.leftKeys.map(_.expr.sig) == outerLk.map(_.expr.sig) &&
        innerRk.map(_.expr.sig) == outerRk2.map(_.expr.sig)
    if !sameKeys then
      return None
    val innerCond = and(innerSide.conds.map(rename(_, m1)) ++ inner.residual.map(rename(_, m1)))
    val outerCond = and(outerSide.conds.map(rename(_, m2)) ++ outerResidual.map(rename(_, m2)))
    val conds     = innerCond.toSeq ++ outerCond.toSeq
    if !conds.forall(c => PushDownFilters.isDeterministic(c.expr)) then
      return None
    val (matches, exclude) =
      if inner.kind == JoinKind.Semi then
        (innerCond, outerCond)
      else
        (outerCond, innerCond)
    // A NOT EXISTS without a condition excludes every b with a key in T: the semi part never
    // keeps it, and fusing gains nothing over an anti join alone
    exclude.map(e => Fused(build, probe, inner.leftKeys, innerRk, matches, e))

  end fuse

  /**
    * The scan under filters and column-renaming projections, with the filters' conjuncts over the
    * scan's ids
    */
  private def side(p: RuntimePlan): Option[Side] =
    p match
      case s: Scan =>
        Some(Side(s, Nil, Map.empty))
      case Filter(child, cond, _) =>
        side(child).map(s => s.copy(conds = rename(cond, s.ids) :: s.conds))
      case Project(child, exprs, columns) =>
        side(child).flatMap { s =>
          val ids = columns
            .indices
            .map { i =>
              exprs(i).expr match
                case c: ColRef =>
                  Some(columns(i).id.value -> s.ids.getOrElse(c.index, c.index))
                case _ =>
                  None
            }
          if ids.forall(_.isDefined) then
            Some(s.copy(ids = ids.flatten.toMap))
          else
            None
        }
      case _ =>
        None

  private def and(cs: Seq[BExpr]): Option[BExpr] = cs.reduceOption((a, b) =>
    BExpr(AndExpr(a.expr, b.expr))
  )

  /** `e` with its column ids renamed by `m` (others kept) */
  private def rename(e: BExpr, m: Map[Int, Int]): BExpr =
    if m.isEmpty then
      e
    else
      val refs = mutable.BitSet.empty
      VExprs.collectRefs(e.expr, refs)
      if refs.isEmpty then
        e
      else
        val pos = Array.tabulate(refs.max + 1)(i => m.getOrElse(i, i))
        BExpr(VExprs.remap(e.expr, pos))

end SemiAntiProbe

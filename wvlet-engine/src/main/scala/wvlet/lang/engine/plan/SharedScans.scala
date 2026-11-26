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
package wvlet.lang.engine.plan

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.vector.Schema
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Share the filtered scan of a table between an uncorrelated scalar subquery, which the planner
  * evaluates at plan time, and the rest of the query when both filter the table by the same
  * conjuncts (`share_subquery_scans`). TPC-H q22 reads `customer` filtered by its country codes
  * once for the `avg(c_acctbal)` subquery and once more for the main query; TPC-DS q44 reads
  * `store_sales` of one store for its HAVING subquery and twice more for its two rankings.
  *
  * The rewrite reads the table, filtered by the common conjuncts and cut to the columns the query
  * names, as one shared CTE: the subquery computes it at plan time, and the other reads scan its
  * rows instead of the table. Each read keeps its other conjuncts as a filter over the CTE. The
  * filters stay in the tree; the runtime binder binds each in its place ([[Planner.sharedScanOf]]).
  *
  * The CTE has only the columns the query names, so the rewrite applies only when no column can
  * reach the output unnamed: the query selects its output explicitly, and has no `*`, set
  * operations, natural joins, pivots, inspections (`describe`), or aggregations without a selection
  * (whose default output lists every input column)
  */
private[engine] object SharedScans extends LogSupport:

  /**
    * A filter over a catalog table
    *
    * @param table
    *   the table node under the filter's wrappers (braces, aliases)
    * @param conds
    *   the filter's conjuncts
    * @param canon
    *   each conjunct bound over the table's columns, without names, when it binds there and has no
    *   subquery: of the same signature for conjuncts that compare the same values ([[same]])
    * @param subs
    *   the scalar subqueries the filter is in, innermost first
    */
  private case class Site(
      filter: Filter,
      table: Relation,
      leafName: String,
      alias: Option[String],
      source: TableSource,
      conds: List[Expression],
      canon: List[Option[VExpr]],
      subs: List[Relation]
  )

  /**
    * Whether two conjuncts compare the same values: equal signatures, as passes match conditions
    * (scalar functions hold closures, so bound conjuncts of the same text aren't equal)
    */
  private def same(a: VExpr, b: VExpr): Boolean = a.sig == b.sig

  /**
    * `r` under the CTEs of its shared scans, with the filters that read them registered in the
    * planner, or `r` itself when no scan is shared
    */
  def rewrite(planner: Planner, r: Relation): Relation =
    try
      rewriteSites(planner, r)
    catch
      case NonFatal(e) =>
        debug(s"Not sharing subquery scans: ${e.getMessage}")
        r

  private def rewriteSites(planner: Planner, r: Relation): Relation =
    val cteNames = planner.cteNamesIn(r)
    val sites    = mutable.ArrayBuffer.empty[Site]
    // Lower-case names the query refers to: the columns a shared CTE must keep
    val names = mutable.Set.empty[String]
    // Aggregations followed by a selection
    val selected = java.util.IdentityHashMap[GroupBy, Unit]()
    // Scalar subqueries of an aggregation's condition, with the filter of the aggregation's input
    val havingInputs = java.util.IdentityHashMap[Relation, Filter]()
    var unsafe       = false

    def markSelected(child: Relation): Unit =
      child match
        case g: GroupBy =>
          selected.put(g, ())
        case f: Filter =>
          markSelected(f.child)
        case _ =>

    def walk(x: Any, subs: List[Relation]): Unit =
      x match
        case s: SubQueryExpression =>
          walk(s.query, s.query :: subs)
        // count(*) reads no column
        case a: FunctionArg if a.value.isInstanceOf[AllColumns] =>
        case _: AllColumns | _: NaturalJoin                     =>
          unsafe = true
        case i: Identifier =>
          names += i.leafName.toLowerCase
        case e: Expression =>
          e.productIterator.foreach(walk(_, subs))
        case _: SetOperation | _: RelationInspector | _: Pivot | _: Unpivot =>
          unsafe = true
        case g: GroupBy if !selected.containsKey(g) =>
          unsafe = true
        case f: Filter =>
          f.child match
            case GroupBy(in: Filter, _, _) =>
              f.filterExpr
                .traverseExpressions { case q: SubQueryExpression =>
                  havingInputs.put(q.query, in)
                }
            case _ =>
          site(planner, f, cteNames, subs).foreach(sites += _)
          f.productIterator.foreach(walk(_, subs))
        case p: LogicalPlan =>
          p match
            case a: Agg =>
              markSelected(a.child)
            case p: Project =>
              markSelected(p.child)
            case c: Count =>
              markSelected(c.child)
            case _ =>
          p.productIterator.foreach(walk(_, subs))
        case Some(v) =>
          walk(v, subs)
        case s: Iterable[?] =>
          s.foreach(walk(_, subs))
        case _ =>

    walk(r, Nil)
    if unsafe || !selectsOutput(r) then
      r
    else
      // A filter in an uncorrelated scalar subquery (or in one nested in such) is evaluated at
      // plan time. Checked only for tables read more than once, since the check plans the subquery
      val uncorrelated               = java.util.IdentityHashMap[Relation, Boolean]()
      def planTime(s: Site): Boolean = s
        .subs
        .exists { q =>
          if !uncorrelated.containsKey(q) then
            uncorrelated.put(q, planner.isUncorrelated(q))
          uncorrelated.get(q)
        }
      val reads = sites.groupMapReduce(_.source.key)(_ => 1)(_ + _)
      // having_totals_from_groups computes the groups once when a subquery of an aggregation's
      // condition reads the aggregation's own input: such a pair isn't shared
      def totaled(s: Site, o: Site): Boolean =
        planner.enabled(Optimization.HavingTotalsFromGroups) &&
          s.subs.headOption.exists(q => havingInputs.get(q) eq o.filter) &&
          s.canon.forall(_.isDefined) && o.canon.forall(_.isDefined) &&
          s.canon.flatten.map(_.sig).toSet == o.canon.flatten.map(_.sig).toSet
      val shares = sharedGroups(
        sites.toList.filter(s => reads(s.source.key) > 1),
        planTime,
        totaled
      )
      if shares.isEmpty then
        r
      else
        val defs = shares.map { (members, common) =>
          val name = planner.hiddenName("scan")
          members.foreach(m => planner.readSharedScan(m.filter, siteOverCTE(m, common, name)))
          planner.addCTEReads(name, members.size)
          debug(
            s"Sharing ${members.size} filtered scans of ${members.head.leafName} as CTE ${name}"
          )
          cteDef(name, members.head, common, names)
        }
        // The filters stay in place: the runtime binder reads each over its CTE
        // ([[Planner.sharedScanOf]]), since rewriting the tree would copy the subqueries
        WithQuery(false, defs, r, NoSpan)
    end if

  end rewriteSites

  /** Whether the output columns of `r` are a selection, so that each is named */
  private def selectsOutput(r: Relation): Boolean =
    r match
      case q: Query =>
        selectsOutput(q.body)
      case w: WithQuery =>
        selectsOutput(w.queryBody)
      case b: BracedRelation =>
        selectsOutput(b.child)
      case s: Sort =>
        selectsOutput(s.child)
      case l: Limit =>
        selectsOutput(l.child)
      case o: Offset =>
        selectsOutput(o.child)
      case d: Distinct =>
        selectsOutput(d.child)
      case f: Filter =>
        selectsOutput(f.child)
      case _: Project | _: Agg | _: Count =>
        true
      case _ =>
        false

  /** The site of a filter directly over a catalog table (not a CTE), under braces and aliases */
  private def site(
      planner: Planner,
      f: Filter,
      cteNames: Set[String],
      subs: List[Relation]
  ): Option[Site] =
    def tableOf(r: Relation): Option[(Relation, String, String, Option[String])] =
      r match
        case b: BracedRelation =>
          tableOf(b.child)
        case q: Query =>
          tableOf(q.body)
        case a: AliasedRelation if a.columnNames.isEmpty =>
          tableOf(a.child).map(t => t.copy(_4 = Some(a.alias.leafName)))
        case t: TableRef if !cteNames.contains(t.name.leafName.toLowerCase) =>
          Some((t, t.name.leafName, t.name.fullName, None))
        case t: TableScan if !cteNames.contains(t.name.name.toLowerCase) =>
          Some((t, t.name.name, t.name.fullName, None))
        case _ =>
          None
    tableOf(f.child).flatMap { (table, leaf, fullName, alias) =>
      val source =
        try
          Some(planner.tableSource(leaf, fullName))
        catch
          case NonFatal(_) =>
            None
      source.map { src =>
        val schema = src.schema.withQualifier(alias.getOrElse(leaf))
        val conds  = planner.splitConjuncts(f.filterExpr)
        val canon  = conds.map { c =>
          if planner.hasSubquery(c) then
            None
          else
            try
              Some(VExprs.mapRefs(Binder(planner, BindScope(schema)).bind(c), _.copy(name = "")))
            catch
              case NonFatal(_) =>
                None
        }
        Site(f, table, leaf, alias, src, conds, canon, subs)
      }
    }

  end site

  /**
    * Groups of sites of the same table that share a scan, with their common conjuncts: each group
    * has a plan-time site and another, and its common conjuncts are estimated to keep at most
    * [[CardinalityEstimator.DefaultSelectivity]] of the table's rows, so that the CTE is small
    */
  private def sharedGroups(
      sites: List[Site],
      planTime: Site => Boolean,
      totaled: (Site, Site) => Boolean
  ): List[(List[Site], List[VExpr])] =
    val used   = mutable.Set.empty[Int]
    val result = List.newBuilder[(List[Site], List[VExpr])]
    sites
      .indices
      .foreach { i =>
        val s = sites(i)
        if !used.contains(i) && planTime(s) then
          var common  = s.canon.flatten.distinctBy(_.sig)
          val members = mutable.ArrayBuffer(i)
          sites
            .indices
            .foreach { j =>
              val o = sites(j)
              if j != i && !used.contains(j) && o.source.key == s.source.key && !totaled(s, o) then
                val c = common.filter(v => o.canon.flatten.exists(same(_, v)))
                if c.nonEmpty then
                  common = c
                  members += j
            }
          val group            = members.map(sites).toList
          lazy val selectivity = ScanFilters.estimatedSelectivity(
            common,
            s.source,
            k => s.source.schema(k).name,
            values = true
          )
          if group.exists(!planTime(_)) && common.nonEmpty &&
            selectivity <= CardinalityEstimator.DefaultSelectivity
          then
            used ++= members
            result += group -> common
      }
    result.result()

  end sharedGroups

  /**
    * The CTE of the shared scan: the table under `first`'s alias, filtered by `first`'s conjuncts
    * in `common`, with the table's columns the query names (`names`)
    */
  private def cteDef(
      name: String,
      first: Site,
      common: List[VExpr],
      names: collection.Set[String]
  ): AliasedRelation =
    val conds = commonConds(first, common)
    val table =
      first.alias match
        case Some(a) =>
          AliasedRelation(first.table, UnquotedIdentifier(a, NoSpan), None, NoSpan)
        case None =>
          first.table
    val fields = first.source.schema.fields
    val kept   = fields.filter(f => names.contains(f.name.toLowerCase))
    val cols   =
      if kept.isEmpty then
        fields.take(1)
      else
        kept
    val items =
      cols
        .map(f => SingleColumn(NameExpr.EmptyName, UnquotedIdentifier(f.name, NoSpan), NoSpan))
        .toList
    AliasedRelation(
      Project(Filter(table, conds.reduce((a, b) => And(a, b, NoSpan)), NoSpan), items, NoSpan),
      UnquotedIdentifier(name, NoSpan),
      None,
      NoSpan
    )

  end cteDef

  private def commonConds(s: Site, common: List[VExpr]): List[Expression] = s
    .conds
    .zip(s.canon)
    .collect {
      case (c, Some(v)) if common.exists(same(_, v)) =>
        (c, v)
    }
    .distinctBy(_._2.sig)
    .map(_._1)

  /**
    * The site's filter over the CTE `name` in place of its table, read under the table's name, with
    * the site's conjuncts not in `common`
    */
  private def siteOverCTE(s: Site, common: List[VExpr], name: String): Relation =
    val cte = AliasedRelation(
      TableRef(UnquotedIdentifier(name, NoSpan), NoSpan),
      UnquotedIdentifier(s.leafName, NoSpan),
      None,
      NoSpan
    )
    val child = s
      .filter
      .child
      .transform {
        case t if t eq s.table =>
          cte
      }
      .asInstanceOf[Relation]
    val rest = s
      .conds
      .zip(s.canon)
      .collect {
        case (c, v) if !v.exists(x => common.exists(same(_, x))) =>
          c
      }
    if rest.isEmpty then
      child
    else
      Filter(child, rest.reduce((a, b) => And(a, b, NoSpan)), s.filter.span)

end SharedScans

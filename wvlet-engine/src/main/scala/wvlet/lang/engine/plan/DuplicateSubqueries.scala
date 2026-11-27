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
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Compute identical aggregating subqueries of a FROM list once, as a shared CTE
  * (`share_duplicate_subqueries`). TPC-DS q65 aggregates `store_sales` by store and item twice:
  * once for the per-store average and once for the rows it compares with that average. Each copy
  * reads the table and builds 2.3M groups at SF10; with the rewrite, both read one result.
  *
  * A subquery is an aliased relation over an aggregation, with no column list. Two are identical
  * when their bound runtime plans have the same key
  * ([[wvlet.lang.engine.runtime.ShareSubplans.key]]), so subqueries with subquery expressions,
  * outer references (correlated), non-deterministic expressions, or reads of CTEs (whose rows
  * depend on their readers) are never shared. The rewrite replaces each copy with a reference to a
  * CTE of the first copy, under the copy's own alias, so that the query resolves its names as
  * before. A subquery inside a shared copy is left alone, since the CTE computes it once.
  *
  * Only aggregations are shared: their results are small next to their inputs, so reading the
  * materialized result costs less than computing it again.
  */
private[engine] object DuplicateSubqueries extends LogSupport:

  /** `r` under the CTEs of its shared subqueries, or `r` itself when none is shared */
  def rewrite(planner: Planner, r: Relation): Relation =
    try
      rewriteCopies(planner, r)
    catch
      case NonFatal(e) =>
        debug(s"Not sharing duplicate subqueries: ${e.getMessage}")
        r

  /**
    * A subquery that may be shared
    *
    * @param rel
    *   the aliased subquery
    * @param outer
    *   the candidates `rel` is nested in, innermost first
    */
  private case class Candidate(rel: AliasedRelation, outer: List[AliasedRelation])

  private def rewriteCopies(planner: Planner, r: Relation): Relation =
    val cteNames   = planner.cteNamesIn(r)
    val candidates = mutable.ArrayBuffer.empty[Candidate]

    // Relations only, not expressions: a subquery expression is planned on its own, and may be
    // correlated with the relation around it
    def walk(x: Any, outer: List[AliasedRelation]): Unit =
      x match
        case _: Expression =>
        case w: WithQuery  =>
          w.queryDefs.foreach(walk(_, outer))
          walk(w.queryBody, outer)
        case a: AliasedRelation
            if a.columnNames.isEmpty && isCandidate(planner, a.child, cteNames) =>
          candidates += Candidate(a, outer)
          walk(a.child, a :: outer)
        case p: LogicalPlan =>
          p.productIterator.foreach(walk(_, outer))
        case Some(v) =>
          walk(v, outer)
        case s: Iterable[?] =>
          s.foreach(walk(_, outer))
        case _ =>

    walk(r, Nil)
    // Bound only when another candidate reads the same tables: binding assigns column ids, which
    // would shift the plans of queries with nothing to share
    val all       = candidates.toList
    val sameReads = all.groupBy(c => tablesOf(c.rel.child)).filter(_._2.size > 1)
    val keyed     = all
      .filter(c => sameReads.contains(tablesOf(c.rel.child)))
      .flatMap(c => planner.inputKey(c.rel.child).map(_ -> c))
    // Copies by key, in query order, outermost first: a copy nested in a shared one is computed by
    // the CTE of its outer copy
    val byKey  = keyed.groupMap(_._1)(_._2)
    val groups = keyed.map(_._1).distinct.map(byKey).filter(_.size > 1)
    val shared = java.util.IdentityHashMap[AliasedRelation, String]()
    groups
      .sortBy(g => g.map(_.outer.size).max)
      .foreach { g =>
        val free = g.filter(c => !c.outer.exists(shared.containsKey))
        if free.size > 1 then
          val name = planner.hiddenName("subquery")
          free.foreach(c => shared.put(c.rel, name))
      }
    // A copy inside a copy shared later is replaced with its outer copy, so a CTE may be left
    // with one reader: such a CTE is dropped, until every CTE has two readers
    var dropping = true
    while dropping do
      val readers =
        all
          .filter(c => shared.containsKey(c.rel) && !c.outer.exists(shared.containsKey))
          .groupMapReduce(c => shared.get(c.rel))(_ => 1)(_ + _)
      val single = all.filter(c =>
        shared.containsKey(c.rel) && readers.getOrElse(shared.get(c.rel), 0) < 2
      )
      single.foreach(c => shared.remove(c.rel))
      dropping = single.nonEmpty
    if shared.isEmpty then
      r
    else
      val defs = mutable.LinkedHashMap.empty[String, AliasedRelation]
      candidates.foreach { c =>
        Option(shared.get(c.rel)).foreach { name =>
          if !defs.contains(name) then
            defs(name) = AliasedRelation(
              c.rel.child,
              UnquotedIdentifier(name, NoSpan),
              None,
              NoSpan
            )
        }
      }
      val body = r
        .transform {
          case a: AliasedRelation if shared.containsKey(a) =>
            a.copy(child = TableRef(UnquotedIdentifier(shared.get(a), NoSpan), NoSpan))
        }
        .asInstanceOf[Relation]
      defs.keys.foreach(n => debug(s"Sharing duplicate subqueries as CTE ${n}"))
      WithQuery(false, defs.values.toList, body, NoSpan)
    end if

  end rewriteCopies

  /** The names of the tables `r` reads, in order, as a cheap test for identical subqueries */
  private def tablesOf(r: Relation): List[String] =
    val names = List.newBuilder[String]
    r.traverse {
      case t: TableRef =>
        names += t.name.fullName.toLowerCase
      case t: TableScan =>
        names += t.name.fullName.toLowerCase
    }
    names.result()

  /** Whether `child` reads no CTE and its rows are an aggregation's (under a selection or sort) */
  private def isCandidate(planner: Planner, child: Relation, cteNames: Set[String]): Boolean =
    def aggregates(r: Relation): Boolean =
      r match
        case _: GroupBy | _: Agg =>
          true
        case q: Query =>
          aggregates(q.body)
        case b: BracedRelation =>
          aggregates(b.child)
        case p: Project =>
          aggregates(p.child)
        case f: Filter =>
          aggregates(f.child)
        case s: Sort =>
          aggregates(s.child)
        case l: Limit =>
          aggregates(l.child)
        case _ =>
          false
    aggregates(child) && !planner.readsCTE(child, cteNames)

end DuplicateSubqueries

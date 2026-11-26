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
import wvlet.lang.engine.exec.Aggregates
import wvlet.lang.engine.vector.PType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Reuse the groups of an IN subquery for the aggregation it filters
  * (`reuse_in_subquery_aggregates`, DuckDB's aggregate reuse). TPC-H q18 aggregates
  * `customer ⋈ orders ⋈ lineitem` filtered by `o_orderkey in (from lineitem group by l_orderkey
  * where l_quantity.sum > 300 select l_orderkey)`, and sums `l_quantity` per order: the subquery
  * and the aggregation both read lineitem.
  *
  * When the aggregation's calls are sums (over integers or decimals), mins, or maxes of one table's
  * columns, that table joins the rest on the subquery's grouping key, and the IN target equals that
  * key, the table is replaced by the subquery's own groups, with each aggregated column holding its
  * per-key total. The outer aggregation then totals those per-key values: a sum of sums, the min of
  * mins, the max of maxes. The groups keep only the keys the subquery selects, so the IN filter is
  * implied by the join and dropped. The table is read once, at run time, instead of once by the
  * subquery at plan time and once more by the join.
  *
  * The rewrite is safe for any multiplicity of the other tables' rows: each of their rows met every
  * table row of its key, and now meets the key's total once.
  */
private[engine] object InSubqueryAggregates extends LogSupport:

  /** A catalog table leaf of a join tree, under braces and aliases */
  private case class Leaf(node: Relation, leafName: String, fullName: String, alias: Option[String])

  /**
    * The input of an aggregation grouping `groupBy`'s input (with the aggregation condition
    * `having`, selecting `items`) with the IN subquery's table replaced by the subquery's groups,
    * or None when the rewrite doesn't apply
    */
  def rewrite(
      planner: Planner,
      groupBy: GroupBy,
      having: Option[Expression],
      items: List[Attribute]
  ): Option[Relation] =
    try
      rewriteInput(planner, groupBy, having, items)
    catch
      case NonFatal(e) =>
        debug(s"Not reusing IN subquery aggregates: ${e.getMessage}")
        None

  private def rewriteInput(
      planner: Planner,
      groupBy: GroupBy,
      having: Option[Expression],
      items: List[Attribute]
  ): Option[Relation] =
    groupBy.child match
      case f: Filter if planner.sharedScanOf(f).isEmpty =>
        val conds    = planner.splitConjuncts(f.filterExpr)
        val cteNames = planner.cteNamesIn(groupBy)
        conds
          .indices
          .iterator
          .flatMap { i =>
            conds(i) match
              case in: InRelation =>
                rewriteWith(
                  planner,
                  f,
                  in,
                  conds.patch(i, Nil, 1),
                  groupBy,
                  having,
                  items,
                  cteNames
                )
              case _ =>
                None
          }
          .nextOption()
      case _ =>
        None

  /** The parts of an IN subquery `from t group by k [where cond] select k` */
  private case class Sub(groups: Relation, table: Leaf, key: String, item: Attribute)

  private def subOf(r: Relation, cteNames: Set[String]): Option[Sub] =
    r match
      case q: Query =>
        subOf(q.body, cteNames)
      case b: BracedRelation =>
        subOf(b.child, cteNames)
      case Project(groups, List(item), _) =>
        def keyOf(g: GroupBy): Option[String] =
          g.groupingKeys match
            case List(k: UnresolvedGroupingKey) =>
              k.child match
                case i: Identifier =>
                  Some(i.leafName)
                case _ =>
                  None
            case _ =>
              None
        val grouped =
          groups match
            case g: GroupBy =>
              Some(g)
            case Filter(g: GroupBy, _, _) =>
              Some(g)
            case _ =>
              None
        for
          g     <- grouped
          key   <- keyOf(g)
          table <- leafOf(g.child, cteNames)
          if selectedName(item).exists(_.equalsIgnoreCase(key))
        yield Sub(groups, table, key, item)
      case _ =>
        None

  /** The column a select item reads when it is a bare column */
  private def selectedName(a: Attribute): Option[String] =
    a match
      case s: SingleColumn =>
        s.expr match
          case i: Identifier =>
            Some(i.leafName)
          case _ =>
            None
      case i: Identifier =>
        Some(i.leafName)
      case _ =>
        None

  private def leafOf(r: Relation, cteNames: Set[String]): Option[Leaf] =
    r match
      case b: BracedRelation =>
        leafOf(b.child, cteNames).map(_.copy(node = b))
      case a: AliasedRelation if a.columnNames.isEmpty =>
        leafOf(a.child, cteNames).map(_.copy(node = a, alias = Some(a.alias.leafName)))
      case t: TableRef if !cteNames.contains(t.name.leafName.toLowerCase) =>
        Some(Leaf(t, t.name.leafName, t.name.fullName, None))
      case t: TableScan if !cteNames.contains(t.name.name.toLowerCase) =>
        Some(Leaf(t, t.name.name, t.name.fullName, None))
      case _ =>
        None

  /**
    * The leaves of an inner-join tree, with its join conditions, or None when it has another kind
    * of join or a leaf that is no catalog table
    */
  private def joinLeaves(
      planner: Planner,
      r: Relation,
      cteNames: Set[String]
  ): Option[(List[Leaf], List[Expression])] =
    r match
      case j: Join
          if !j.asof && (
            j.joinType == JoinType.InnerJoin || j.joinType == JoinType.ImplicitJoin ||
              j.joinType == JoinType.CrossJoin
          ) =>
        val cond =
          j.cond match
            case NoJoinCriteria =>
              Some(Nil)
            case JoinOn(e, _) =>
              Some(planner.splitConjuncts(e))
            case _ =>
              None
        for
          c       <- cond
          (l, a)  <- joinLeaves(planner, j.left, cteNames)
          (rr, b) <- joinLeaves(planner, j.right, cteNames)
        yield (l ++ rr, a ++ b ++ c)
      case other =>
        leafOf(other, cteNames).map(l => (List(l), Nil))

  /** An aggregate call: its lower-case name and its arguments, receiver first */
  private def aggregateOf(f: FunctionApply): Option[(String, List[FunctionArg])] =
    val name = Binder.functionName(f)
    Option.when(f.window.isEmpty && Aggregates.isAggregate(name)) {
      val receiver =
        f.base match
          case d: DotRef =>
            List(FunctionArg(None, d.qualifier, false, Nil, NoSpan))
          case _ =>
            Nil
      (name, receiver ++ f.args)
    }

  private def rewriteWith(
      planner: Planner,
      filter: Filter,
      in: InRelation,
      others: List[Expression],
      groupBy: GroupBy,
      having: Option[Expression],
      items: List[Attribute],
      cteNames: Set[String]
  ): Option[Relation] =
    for
      sub               <- subOf(in.in, cteNames)
      (leaves, onConds) <- joinLeaves(planner, filter.child, cteNames)
      result <- replace(planner, filter, in, others, onConds, leaves, sub, groupBy, having, items)
    yield result

  private def replace(
      planner: Planner,
      filter: Filter,
      in: InRelation,
      others: List[Expression],
      onConds: List[Expression],
      leaves: List[Leaf],
      sub: Sub,
      groupBy: GroupBy,
      having: Option[Expression],
      items: List[Attribute]
  ): Option[Relation] =
    val subSource = planner.tableSource(sub.table.leafName, sub.table.fullName)
    val sources   = leaves.map(l => l -> planner.tableSource(l.leafName, l.fullName))
    // The outer leaf reading the subquery's table, which must be read once
    val matching = sources.filter(_._2.key == subSource.key)
    if leaves.size < 2 || matching.size != 1 then
      return None
    val (leaf, source)                                                 = matching.head
    def columns(s: wvlet.lang.engine.catalog.TableSource): Set[String] =
      s.schema.fields.map(_.name.toLowerCase).toSet
    val tableColumns = columns(source)
    val otherColumns = sources.filterNot(_._1 eq leaf).flatMap(x => columns(x._2)).toSet
    val key          = sub.key.toLowerCase
    // Columns the aggregation reads outside aggregate calls, and its calls
    val plainRefs = mutable.Set.empty[String]
    val calls     = mutable.ArrayBuffer.empty[(String, List[FunctionArg], FunctionApply)]
    var unsafe    = false
    def visit(e: Expression): Unit =
      e match
        case f: FunctionApply =>
          aggregateOf(f) match
            case Some((name, args)) =>
              if f.filter.isDefined then
                unsafe = true
              calls += ((name, args, f))
            case None =>
              f.base match
                case d: DotRef =>
                  visit(d.qualifier)
                case _: Identifier =>
                case other         =>
                  visit(other)
              f.args.foreach(visit)
              f.window.foreach(visit)
              f.filter.foreach(visit)
        case _: WindowApply | _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation |
            _: AllColumns =>
          unsafe = true
        case d: DotRef =>
          plainRefs += d.name.leafName.toLowerCase
          d.qualifier match
            case q: Identifier =>
              // A table qualifier, or a struct column whose field is read
              plainRefs += q.leafName.toLowerCase
            case q =>
              visit(q)
        case i: Identifier =>
          plainRefs += i.leafName.toLowerCase
        case other =>
          other.children.foreach(visit)
    // sql"..." templates (stdlib aggregates such as `sum`) as the calls they expand to
    (groupBy.groupingKeys ++ items ++ having ++ others ++ onConds).foreach(e =>
      visit(Binder.expandTemplates(e))
    )
    // The aggregated columns, each by one of sum, min, and max over the table's column alone
    val aggregated = mutable.LinkedHashMap.empty[String, FunctionApply]
    calls.foreach { (name, args, f) =>
      val col =
        args match
          case List(a) if a.isPlainArg =>
            a.value match
              case i: Identifier =>
                Some(i.leafName.toLowerCase)
              case d: DotRef if d.qualifier.isInstanceOf[Identifier] =>
                Some(d.name.leafName.toLowerCase)
              case _ =>
                None
          case _ =>
            None
      col match
        case Some(c) if tableColumns.contains(c) && c != key =>
          val exact =
            name match
              case "min" | "max" =>
                true
              case "sum" =>
                source
                  .schema
                  .fields
                  .find(_.name.equalsIgnoreCase(c))
                  .exists { f =>
                    f.ptype == PType.Int64 || f.ptype == PType.Int32 ||
                    f.ptype.isInstanceOf[PType.Decimal]
                  }
              case _ =>
                false
          if !exact || aggregated.get(c).exists(g => Binder.functionName(g) != name) then
            unsafe = true
          else if !aggregated.contains(c) then
            aggregated(c) = f
        case _ =>
          // A call over other columns (or count(*)) would see one row per key instead of the
          // table's rows
          unsafe = true
    }
    // The IN target is the subquery's key, or a column a conjunct equates with it
    def isKey(e: Expression): Boolean =
      e match
        case i: Identifier =>
          i.leafName.equalsIgnoreCase(key)
        case d: DotRef =>
          d.name.leafName.equalsIgnoreCase(key)
        case _ =>
          false
    def sameColumn(a: Expression, b: Expression): Boolean =
      (a, b) match
        case (x: Identifier, y: Identifier) =>
          x.leafName.equalsIgnoreCase(y.leafName)
        case _ =>
          false
    def isColumn(e: Expression): Boolean =
      e match
        case _: Identifier | _: DotRef =>
          true
        case _ =>
          false
    // Columns a conjunct equates with the key. Such a conjunct joins on the key and rejects the
    // NULL key's group, which the dropped IN filter would have rejected
    val keyEqs = (others ++ onConds).collect {
      case Eq(l, r, _) if isKey(l) && !isKey(r) && isColumn(r) =>
        r
      case Eq(l, r, _) if isKey(r) && !isKey(l) && isColumn(l) =>
        l
    }
    // The IN target is the key, or a column equated with it
    val targetIsKey = keyEqs.nonEmpty && (isKey(in.a) || keyEqs.exists(sameColumn(_, in.a)))
    val names       = aggregated.keySet.toSet + key
    val ok          =
      !unsafe && aggregated.nonEmpty && targetIsKey && tableColumns.contains(key) &&
        // The table's other columns aren't read, and its column names are unique among the leaves
        plainRefs.forall(c => c == key || !tableColumns.contains(c)) &&
        !names.exists(otherColumns.contains) && !planner.hasSubquery(in.a) &&
        !others.exists(planner.hasSubquery) && !onConds.exists(planner.hasSubquery) &&
        planner.isUncorrelated(in.in)
    Option.when(ok) {
      // The subquery's groups, selecting its key and each aggregated column's total under the
      // column's name, read under the outer leaf's name
      val totals = aggregated.map((c, f) => SingleColumn(NameExpr.fromString(c), f, NoSpan)).toList
      // The key under its own name, even when the subquery selects it under an alias
      val keyItem = SingleColumn(
        NameExpr.fromString(sub.key),
        UnquotedIdentifier(sub.key, NoSpan),
        NoSpan
      )
      val groups = Project(sub.groups, keyItem :: totals, NoSpan)
      val node   = AliasedRelation(
        groups,
        UnquotedIdentifier(leaf.alias.getOrElse(leaf.leafName), NoSpan),
        None,
        NoSpan
      )
      val child = filter
        .child
        .transform {
          case t if t eq leaf.node =>
            node
        }
        .asInstanceOf[Relation]
      debug(s"Reusing the groups of an IN subquery for ${aggregated.keys.mkString(", ")}")
      if others.isEmpty then
        child
      else
        Filter(child, others.reduce((a, b) => And(a, b, NoSpan)), filter.span)
    }

  end replace

end InSubqueryAggregates

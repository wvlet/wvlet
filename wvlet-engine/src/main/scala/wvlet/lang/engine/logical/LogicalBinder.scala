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
package wvlet.lang.engine.logical

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.WvletLangException
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.ScanOp
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.plan.BindScope
import wvlet.lang.engine.plan.Binder
import wvlet.lang.engine.plan.ColumnNotFound
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.vector.Field
import wvlet.lang.engine.vector.Schema
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.{LogicalPlan as _, *}

/**
  * Binds a typed, model-expanded [[Relation]] to a [[LogicalPlan]]: names resolve to column ids
  * once, here. It covers scans, VALUES, filters, projections, sorts, and limits, and the wrappers
  * that only rename or requalify columns. Every other relation becomes a [[LogicalPlan.Legacy]]
  * leaf planned by `planner` (whose own inputs come back here).
  *
  * The binder routes a relation by the same tests the planner uses, and binds inputs in the
  * planner's order, so that the lowered plan is the planner's plan and the planner's state (shared
  * CTEs, hidden column names) changes in the same order. A relation that fails to bind is an error,
  * as in the planner; the binder never falls back to the planner after binding an input.
  */
class LogicalBinder(planner: Planner):
  private var lastId = 0

  private def newColumn(f: Field): Column =
    lastId += 1
    Column(ColumnId(lastId), f.name, f.ptype, f.qualifiers)

  private def columnsOf(s: Schema): IndexedSeq[Column] = s.fields.map(newColumn)

  /** A relation planned by the recursive planner */
  private def legacy(r: Relation): LogicalPlan = legacyOp(planner.planLegacy(r))

  private def legacyOp(op: Operator): LogicalPlan = LogicalPlan.Legacy(op, columnsOf(op.schema))

  def bind(r: Relation): LogicalPlan =
    // Same case order as Planner.planLegacy, so that a relation takes the planner's path
    r match
      case q: Query =>
        bind(q.body)
      case w: WithQuery =>
        planner.registerCTEs(w)
        bind(w.queryBody)
      case b: BracedRelation =>
        bind(b.child)
      case s: SelectAsAlias =>
        bind(s.child)
      case t: TestRelation =>
        bind(t.child)
      case d: Debug =>
        bind(d.child)
      case d: Describe =>
        legacy(d)
      case r: RelationInspector =>
        bind(r.child)
      case p: PartitioningHint =>
        bind(p.child)
      case n: NamedRelation =>
        requalify(bind(n.child), n.name.leafName)
      case e: EmptyRelation =>
        legacy(e)
      case v: Values =>
        val p = values(v, None)
        v.relationType match
          // An inlined table value (val t(a, b) = [...]) is referenced by its name
          case s: DataType.SchemaType if s.isFullyNamed =>
            requalify(p, s.typeName.name)
          case _ =>
            p
      case a: AliasedRelation =>
        aliased(a)
      case t: TableScan =>
        table(t, t.name.name, t.name.fullName)
      case t: TableRef =>
        table(t, t.name.leafName, t.name.fullName)
      case f: FileScan =>
        scan(planner.fileSource(f.filePath), None)
      case f: FileRef =>
        scan(planner.fileSource(f.filePath), None)
      case r: RawSQL =>
        bind(planner.parseSql(r))
      case f: Filter =>
        filter(f)
      case s: Sort =>
        sort(s, None)
      case l: Limit =>
        val n = l.limit.value
        l.child match
          case s: Sort =>
            sort(s, Some(n))
          case Offset(s: Sort, rows, _) =>
            LogicalPlan.Limit(sort(s, Some(n + rows.value)), n, rows.value)
          case Offset(child, rows, _) =>
            LogicalPlan.Limit(bind(child), n, rows.value)
          case child =>
            LogicalPlan.Limit(bind(child), n, 0)
      case o: Offset =>
        o.child match
          case Limit(child, limit, _) =>
            // LIMIT n OFFSET m in SQL order: the offset applies first
            LogicalPlan.Limit(bind(child), limit.value, o.rows.value)
          case child =>
            LogicalPlan.Limit(bind(child), Long.MaxValue, o.rows.value)
      case p: Project if isPlainProjection(p) =>
        project(p)
      case s: SQLSelect =>
        bind(planner.desugarSQLSelect(s))
      case other =>
        // Distinct, aggregations, pivots, unnest, column edits, joins, set operations, samples, a
        // Project that aggregates or evaluates windows, ...
        legacy(other)

  end bind

  private def scan(src: wvlet.lang.engine.catalog.TableSource, alias: Option[String]): LogicalPlan =
    val schema = alias.map(a => src.schema.withQualifier(a)).getOrElse(src.schema)
    LogicalPlan.Scan(src, columnsOf(schema))

  private def table(r: Relation, name: String, fullName: String): LogicalPlan =
    if planner.isCTE(name) then
      // The planner inlines or shares CTEs
      legacy(r)
    else
      scan(planner.tableSource(name, fullName), Some(name))

  private def values(v: Values, names: Option[List[DataType.NamedType]]): LogicalPlan =
    val (schema, batch) = planner.valuesBatch(v, names)
    LogicalPlan.Values(columnsOf(schema), batch)

  /** The same columns under new names and qualifiers, as a projection */
  private def renamed(child: LogicalPlan, columns: IndexedSeq[Column]): LogicalPlan = LogicalPlan
    .Project(
      child,
      child
        .output
        .indices
        .map(i => BExpr(ColRef(child.output(i).id.value, columns(i).ptype, columns(i).name))),
      columns
    )

  private def requalify(p: LogicalPlan, q: String): LogicalPlan = renamed(
    p,
    p.output.map(c => newColumn(Field(c.name, c.ptype, List(q))))
  )

  private def aliased(a: AliasedRelation): LogicalPlan =
    val child =
      a.child match
        case v: Values =>
          values(v, a.columnNames)
        case other =>
          bind(other)
    val names  = a.columnNames.map(_.map(_.name.name))
    val fields = child
      .output
      .zipWithIndex
      .map { (c, i) =>
        Field(names.flatMap(_.lift(i)).getOrElse(c.name), c.ptype, List(a.alias.leafName))
      }
    child match
      case LogicalPlan.Scan(src, _) =>
        LogicalPlan.Scan(src, fields.map(newColumn))
      case LogicalPlan.Legacy(s: ScanOp, _) =>
        legacyOp(s.withSchema(Schema(fields)))
      case _ =>
        renamed(child, fields.map(newColumn))

  private def filter(f: Filter): LogicalPlan =
    f.child match
      case _: GroupBy =>
        // HAVING without a selection
        legacy(f)
      case j: Join if planner.isInnerJoin(j) =>
        // The join tree takes the conditions for its join order and pushdown
        legacy(f)
      case j: Join if j.joinType == JoinType.LeftOuterJoin && !j.asof =>
        legacy(f)
      case child =>
        val conds = planner.conjuncts(f.filterExpr)
        if conds.exists(planner.hasSubquery) then
          // Subqueries become joins over the planned input
          legacy(f)
        else
          val in     = bind(child)
          val schema = in.schema
          val bound  = conds.map(c => Binder(planner, BindScope(schema)).bind(c))
          LogicalPlan.Filter(in, BExpr.of(bound.reduce((a, b) => AndExpr(a, b)), in.output))

  /** A projection without aggregates, windows, unnest, or subqueries */
  private def isPlainProjection(p: Project): Boolean =
    !planner.isAggregationInput(p.child) &&
      p.selectItems
        .forall { i =>
          !Binder.containsAggregate(i) && planner.unnestArg(i).isEmpty &&
          !Binder.containsWindow(i) && !planner.hasSubquery(Binder.expandTemplates(i))
        }

  private def project(p: Project): LogicalPlan =
    val in              = bind(p.child)
    val binder          = Binder(planner, BindScope(in.schema))
    val (exprs, fields) = planner.bindItems(binder, p.selectItems)
    LogicalPlan.Project(in, exprs.map(BExpr.of(_, in.output)), fields.map(newColumn))

  private def sort(s: Sort, limit: Option[Long]): LogicalPlan =
    if s.orderBy.exists(k => planner.hasSubquery(k.sortKey)) then
      legacyOp(planner.planSort(s, limit))
    else
      val in      = bind(s.child)
      val schema  = in.schema
      val orderBy = planner.resolveSortItems(s)
      // Keys over the input's positions
      val outKeys = orderBy.map { k =>
        try
          Right(planner.sortKeys(Binder(planner, BindScope(schema)), List(k), schema).head)
        catch
          case e @ (_: ColumnNotFound | _: WvletLangException) =>
            Left(e)
      }
      def orderKey(k: wvlet.lang.engine.exec.SortKey, over: LogicalPlan): OrderKey = OrderKey(
        BExpr.of(k.expr, over.output),
        k.ascending,
        k.nullsFirst
      )
      if outKeys.forall(_.isRight) then
        LogicalPlan.Sort(in, outKeys.map(k => orderKey(k.toOption.get, in)).toIndexedSeq, limit)
      else
        s.child match
          case p: Project =>
            // ORDER BY an expression outside the select list (an input column, or an aggregate
            // over a grouping): evaluate it as a hidden select item
            val hidden = orderBy
              .zip(outKeys)
              .collect { case (k, Left(_)) =>
                k -> planner.hiddenName("sort")
              }
            val proj = bind(
              p.copy(selectItems =
                p.selectItems ++
                  hidden.map((k, name) =>
                    SingleColumn(NameExpr.fromString(name), k.sortKey, NoSpan)
                  )
              )
            )
            val out        = proj.output
            val hiddenRefs =
              hidden
                .map { (k, name) =>
                  val c  = out.find(_.name == name).get
                  val sk = planner.sortKey(k, ColRef(c.id.value, c.ptype, name))
                  OrderKey(BExpr(sk.expr), sk.ascending, sk.nullsFirst)
                }
                .iterator
            // The select items keep their positions in the extended projection
            val keys = outKeys.map {
              case Right(k) =>
                orderKey(k, proj)
              case Left(_) =>
                hiddenRefs.next()
            }
            val sorted = LogicalPlan.Sort(proj, keys.toIndexedSeq, limit)
            if out.exists(c => planner.isHidden(c.toField)) then
              val keep = out.filterNot(c => planner.isHidden(c.toField))
              LogicalPlan.Project(sorted, keep.map(BExpr.ref), keep.map(c => newColumn(c.toField)))
            else
              sorted
          case _ =>
            throw outKeys
              .collectFirst { case Left(e) =>
                e
              }
              .get
      end if

  end sort

end LogicalBinder

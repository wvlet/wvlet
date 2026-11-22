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
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.SqlGenerator
import wvlet.lang.compiler.DBType
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import scala.collection.mutable

/**
  * Translates a typed, model-expanded wvlet [[Relation]] into a tree of physical [[Operator]]s.
  * Column names are resolved against the runtime schemas of the operators (not the compiler's
  * types), so queries over files with unknown schemas plan the same way as catalog tables.
  */
class Planner(catalog: EngineCatalog, sources: SourceResolver)(using ctx: Context)
    extends LogSupport:

  private var hiddenSeq                          = 0
  private def hiddenName(prefix: String): String =
    hiddenSeq += 1
    s"__wv_${prefix}${hiddenSeq}"

  private def isHidden(f: Field): Boolean = f.name.startsWith("__wv_")

  // Named relations of WITH clauses
  private val cteDefs = mutable.HashMap.empty[String, AliasedRelation]

  /** Window function calls collected while binding a projection */
  private var windowCollector: Option[WindowCollector] = None

  def plan(r: Relation): Operator = planRelation(r)

  // --- relations ------------------------------------------------------------------------

  private def planRelation(r: Relation): Operator =
    r match
      case q: Query =>
        planRelation(q.body)
      case w: WithQuery =>
        w.queryDefs.foreach(d => cteDefs(d.alias.leafName.toLowerCase) = d)
        planRelation(w.queryBody)
      case b: BracedRelation =>
        planRelation(b.child)
      case s: SelectAsAlias =>
        planRelation(s.child)
      case t: TestRelation =>
        planRelation(t.child)
      case d: Debug =>
        planRelation(d.child)
      case d: Describe =>
        val op   = planRelation(d.child)
        val rows = op
          .schema
          .fields
          .filterNot(isHidden)
          .map(f => Seq[Any](f.name, f.ptype.toDataType.typeDescription))
        val schema = Schema(
          IndexedSeq(Field("column_name", PType.Str), Field("column_type", PType.Str))
        )
        BatchesOp(schema, Seq(batchOf(schema, rows)))
      case r: RelationInspector =>
        planRelation(r.child)
      case p: PartitioningHint =>
        planRelation(p.child)
      case n: NamedRelation =>
        requalify(planRelation(n.child), n.name.leafName)
      case _: EmptyRelation =>
        SingleRowOp()
      case v: Values =>
        val op = planValues(v, None)
        v.relationType match
          // An inlined table value (val t(a, b) = [...]) is referenced by its name
          case s: wvlet.lang.model.DataType.SchemaType if s.isFullyNamed =>
            requalify(op, s.typeName.name)
          case _ =>
            op
      case a: AliasedRelation =>
        planAliased(a)
      case t: TableScan =>
        planTable(t.name.name, t.name.fullName)
      case t: TableRef =>
        planTable(t.name.leafName, t.name.fullName)
      case f: FileScan =>
        sources.fileSource(f.filePath) match
          case Some(src) =>
            scanOp(src, None)
          case None =>
            throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported file format: ${f.filePath}")
      case f: FileRef =>
        sources.fileSource(f.filePath) match
          case Some(src) =>
            scanOp(src, None)
          case None =>
            throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported file format: ${f.filePath}")
      case r: RawSQL =>
        planRelation(sources.parseSql(r))
      case f: Filter =>
        planFilter(f)
      case s: Sort =>
        planSort(s, None)
      case l: Limit =>
        l.child match
          case s: Sort =>
            planSort(s, Some(l.limit.value))
          case Offset(s: Sort, rows, _) =>
            LimitOp(planSort(s, Some(l.limit.value + rows.value)), l.limit.value, rows.value)
          case Offset(child, rows, _) =>
            LimitOp(planRelation(child), l.limit.value, rows.value)
          case child =>
            LimitOp(planRelation(child), l.limit.value, 0)
      case o: Offset =>
        o.child match
          case Limit(child, limit, _) =>
            // LIMIT n OFFSET m in SQL order: the offset applies first
            LimitOp(planRelation(child), limit.value, o.rows.value)
          case child =>
            LimitOp(planRelation(child), Long.MaxValue, o.rows.value)
      case d: Distinct =>
        DistinctOp(planRelation(d.child))
      case d: Dedup =>
        DistinctOp(planRelation(d.child))
      case g: GroupBy =>
        planAggregation(g, None, defaultItems = true)
      case Agg(p: Pivot, _, items, _) =>
        planPivot(p, items)
      case p: Pivot =>
        planPivot(p, Nil)
      case u: Unpivot =>
        planUnpivot(u)
      case r if unnestCall(r).isDefined =>
        val (call, alias) = unnestCall(r).get
        planUnnest(None, call, alias)
      case a: Agg =>
        planAggregation(a.child, Some(a.selectItems), defaultItems = false)
      case c: Count if isAggregationInput(c.child) =>
        // Count the groups
        val groups = planAggregation(c.child, None, defaultItems = false)
        val t      = Aggregates.resultType("count_star", Nil)
        val name   = c.selectItems.headOption.map(nameOf).getOrElse("count")
        HashAggOp(
          groups,
          IndexedSeq.empty,
          IndexedSeq(AggCall("count_star", Nil, distinct = false, filter = None, t)),
          Schema(IndexedSeq(Field(name, t)))
        )
      case c: Count =>
        planAggregation(c.child, Some(c.selectItems), defaultItems = false)
      case p: Project =>
        planProject(p.child, p.selectItems)
      case s: SQLSelect =>
        planSQLSelect(s)
      case a: AddColumnsToRelation =>
        planColumnEdit(a.child, prepend = false, a.newColumns)
      case a: PrependColumnsToRelation =>
        planColumnEdit(a.child, prepend = true, a.newColumns)
      case e: ExcludeColumnsFromRelation =>
        val op    = planRelation(e.child)
        val drops = e.columnNames.map(_.leafName.toLowerCase).toSet
        val keep  = op
          .schema
          .fields
          .indices
          .filter(i => !drops.contains(op.schema(i).name.toLowerCase))
        projectIndexes(op, keep)
      case r: RenameColumnsFromRelation =>
        val op      = planRelation(r.child)
        val renames =
          r.columnAliases
            .map { a =>
              val from =
                a.expr match
                  case i: Identifier =>
                    i.leafName
                  case d: DotRef =>
                    d.name.leafName
                  case other =>
                    other.toString
              from.toLowerCase -> a.nameExpr.leafName
            }
            .toMap
        val schema = op
          .schema
          .mapFields(f => renames.get(f.name.toLowerCase).map(f.withName).getOrElse(f))
        ProjectOp(
          op,
          op.schema.fields.indices.map(i => ColRef(i, op.schema(i).ptype, op.schema(i).name)),
          schema
        )
      case s: ShiftColumns =>
        val op            = planRelation(s.child)
        val names         = s.shiftItems.map(_.leafName.toLowerCase)
        val idx           = op.schema.fields.indices
        val (moved, rest) = idx.partition(i => names.contains(op.schema(i).name.toLowerCase))
        val ordered       = names.flatMap(n => moved.find(i => op.schema(i).name.toLowerCase == n))
        projectIndexes(
          op,
          if s.isLeftShift then
            ordered ++ rest
          else
            rest ++ ordered
        )
      case j: Join =>
        planJoinTree(j, Nil)
      case u: Union =>
        val op = UnionAllOp(
          Seq(planRelation(u.left), planRelation(u.right)),
          unionSchema(u.left, u.right)
        )
        if u.isDistinct then
          DistinctOp(op)
        else
          op
      case c: Concat =>
        val inputs = Seq(planRelation(c.left), planRelation(c.right))
        UnionAllOp(inputs, mergeSchemas(inputs.map(_.schema)))
      case i: Intersect =>
        val l  = planRelation(i.left)
        val rr = planRelation(i.right)
        SetOpOp(
          l,
          rr,
          isIntersect = true,
          all = !i.isDistinct,
          mergeSchemas(Seq(l.schema, rr.schema))
        )
      case e: Except =>
        val l  = planRelation(e.left)
        val rr = planRelation(e.right)
        SetOpOp(
          l,
          rr,
          isIntersect = false,
          all = !e.isDistinct,
          mergeSchemas(Seq(l.schema, rr.schema))
        )
      case s: Sample =>
        planSample(s)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported relation in the engine: ${other.nodeName}")

  end planRelation

  private def batchOf(schema: Schema, rows: Seq[Seq[Any]]): Batch =
    val cols = schema
      .fields
      .indices
      .map { c =>
        Vec.fromValues(schema(c).ptype, rows.map(_(c)))
      }
    Batch(cols.toArray, rows.size)

  private def requalify(op: Operator, q: String): Operator =
    val schema = op.schema.withQualifier(q)
    ProjectOp(
      op,
      schema.fields.indices.map(i => ColRef(i, schema(i).ptype, schema(i).name)),
      schema
    )

  private def projectIndexes(op: Operator, idx: Seq[Int]): Operator = ProjectOp(
    op,
    idx.map(i => ColRef(i, op.schema(i).ptype, op.schema(i).name)).toIndexedSeq,
    Schema(idx.map(op.schema(_)).toIndexedSeq)
  )

  /** Drop hidden helper columns */
  private def dropHidden(op: Operator): Operator =
    if op.schema.fields.exists(isHidden) then
      projectIndexes(op, op.schema.fields.indices.filterNot(i => isHidden(op.schema(i))))
    else
      op

  private def mergeSchemas(schemas: Seq[Schema]): Schema =
    val first = schemas.head
    Schema(
      first
        .fields
        .indices
        .map { i =>
          val t = schemas
            .map(s =>
              if i < s.size then
                s(i).ptype
              else
                PType.Null
            )
            .reduce(PType.common)
          Field(first(i).name, t)
        }
    )

  private def unionSchema(l: Relation, r: Relation): Schema = mergeSchemas(
    Seq(planRelation(l).schema, planRelation(r).schema)
  )

  private def planTable(name: String, fullName: String): Operator =
    cteDefs.get(name.toLowerCase) match
      case Some(d) =>
        planAliased(d)
      case None =>
        val src = catalog
          .find(name)
          .orElse(catalog.find(fullName))
          .orElse(sources.tableSource(name, fullName))
          .getOrElse {
            throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${fullName}")
          }
        scanOp(src, Some(name))

  private def scanOp(src: TableSource, alias: Option[String]): Operator =
    val schema = alias.map(a => src.schema.withQualifier(a)).getOrElse(src.schema)
    ScanOp(src, schema, None, None)

  private def planAliased(a: AliasedRelation): Operator =
    val child =
      a.child match
        case v: Values =>
          planValues(v, a.columnNames)
        case other =>
          planRelation(other)
    val names  = a.columnNames.map(_.map(_.name.name))
    val schema = Schema(
      child
        .schema
        .fields
        .zipWithIndex
        .map { (f, i) =>
          val n = names.flatMap(_.lift(i)).getOrElse(f.name)
          Field(n, f.ptype, List(a.alias.leafName))
        }
    )
    child match
      case s: ScanOp =>
        s.withSchema(schema)
      case _ =>
        ProjectOp(
          child,
          schema.fields.indices.map(i => ColRef(i, schema(i).ptype, schema(i).name)),
          schema
        )

  private def planValues(
      v: Values,
      columnNames: Option[List[wvlet.lang.model.DataType.NamedType]]
  ): Operator =
    val binder                  = Binder(this, BindScope(Schema.empty))
    val rows: List[List[VExpr]] = v
      .rows
      .map {
        case a: ArrayConstructor =>
          a.values.map(binder.bind)
        case r: RowConstructor =>
          r.values.map(binder.bind)
        case other =>
          List(binder.bind(other))
      }
    val width                     = rows.headOption.map(_.size).getOrElse(0)
    val names: IndexedSeq[String] = (0 until width).map { i =>
      columnNames
        .flatMap(_.lift(i))
        .map(_.name.name)
        .orElse {
          v.schema match
            case s: wvlet.lang.model.DataType.SchemaType =>
              s.columnTypes.lift(i).map(_.name.name).filter(n => n.nonEmpty && !n.startsWith("<"))
            case _ =>
              None
        }
        .getOrElse(s"col${i}")
    }
    val types = (0 until width).map { i =>
      val t = rows.map(_(i).ptype).reduceOption(PType.common).getOrElse(PType.Null)
      if t == PType.Null then
        PType.Int32
      else
        t
    }
    val schema = Schema(names.indices.map(i => Field(names(i), types(i))))
    val one    = Batch.ofRows(1)
    val values = rows.map(r =>
      r.zipWithIndex.map((e, i) => ValueOps.cast(e.eval(one).get(0), e.ptype, types(i)))
    )
    BatchesOp(schema, Seq(batchOf(schema, values)), "Values")

  end planValues

  // --- pivot / unpivot -----------------------------------------------------------------

  /** Column indexes referenced by an expression */
  private def columnRefs(e: VExpr): Set[Int] =
    e match
      case c: ColRef =>
        Set(c.index)
      case other =>
        other.children.flatMap(columnRefs).toSet

  /**
    * PIVOT with DuckDB semantics: one output column per pivot value (combination), each an
    * aggregate filtered to its value. Without USING aggregates, count(*). Without GROUP BY, rows
    * are grouped by the columns used neither for pivoting nor in the aggregates
    */
  private def planPivot(p: Pivot, aggItems: List[Attribute]): Operator =
    val childOp = planRelation(p.child)
    val schema  = childOp.schema
    val batch   = Operator.materialize(childOp)
    def input   = BatchesOp(schema, Seq(batch))
    val scope   = BindScope(schema)
    val binder  = Binder(this, scope)
    // Pivot columns and their values
    val pivots: List[(VExpr, Seq[Any])] = p
      .pivotKeys
      .map { k =>
        val col    = binder.bind(k.name)
        val values =
          if k.values.nonEmpty then
            k.values
              .map { v =>
                val c = binder.bind(v)
                ValueOps.cast(c.eval(Batch.ofRows(1)).get(0), c.ptype, col.ptype)
              }
          else
            val vec = col.eval(batch)
            (0 until batch.size)
              .map(vec.get)
              .filter(_ != null)
              .distinct
              .sortWith((x, y) => ValueOps.compare(x, y) < 0)
        (col, values)
      }
    val combos: Seq[List[Any]] =
      pivots.foldRight(Seq(List.empty[Any])) { case ((_, vs), acc) =>
        for
          v    <- vs
          rest <- acc
        yield v :: rest
      }
    // USING aggregates
    val aggCtx0    = AggContext(scope, IndexedSeq.empty, IndexedSeq.empty)
    val aggBinder0 = Binder(this, BindScope(aggCtx0.outputSchema), Some(aggCtx0))
    def aggFunction(e: Expression): FunctionApply =
      Binder.expandTemplates(e) match
        case f: FunctionApply =>
          f
        case a: SingleColumn =>
          aggFunction(a.expr)
        case a: Alias =>
          aggFunction(a.expr)
        case other =>
          throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported pivot aggregate: ${other}")
    val countStar = AggCall(
      "count_star",
      Nil,
      distinct = false,
      filter = None,
      Aggregates.resultType("count_star", Nil)
    )
    val aggs: List[(String, AggCall)] =
      if aggItems.isEmpty then
        List(("count", countStar))
      else
        aggItems.map(a => (nameOf(a), aggBinder0.aggCall(aggFunction(a))))
    // Grouping keys
    val pivotCols                         = pivots.flatMap(x => columnRefs(x._1)).toSet
    val usedCols                          = aggs.flatMap(_._2.args.flatMap(columnRefs)).toSet
    val keys: IndexedSeq[(String, VExpr)] =
      if p.groupingKeys.nonEmpty then
        p.groupingKeys
          .map { k =>
            val n    = k.name.leafName
            val name =
              if n.nonEmpty && !n.startsWith("<") then
                n
              else
                exprName(k.child)
            (name, binder.bind(k.child))
          }
          .toIndexedSeq
      else
        schema
          .fields
          .indices
          .filterNot(i => pivotCols.contains(i) || usedCols.contains(i) || isHidden(schema(i)))
          .map(i => (schema(i).name, ColRef(i, schema(i).ptype, schema(i).name): VExpr))
    // One filtered aggregate per (pivot values, aggregate)
    val calls  = IndexedSeq.newBuilder[AggCall]
    val fields = IndexedSeq.newBuilder[Field]
    combos.foreach { combo =>
      val cond = pivots
        .zip(combo)
        .map { case ((col, _), v) =>
          Compare(CmpOp.Eq, col, Const(v, col.ptype)): VExpr
        }
        .reduce((a, b) => AndExpr(a, b))
      val prefix = combo.map(ValueOps.toStr).mkString("_")
      aggs.foreach { (aggName, call) =>
        val filter = call.filter.map(f => AndExpr(f, cond)).getOrElse(cond)
        calls += call.copy(filter = Some(filter))
        fields +=
          Field(
            if aggs.size == 1 then
              prefix
            else
              s"${prefix}_${aggName}"
            ,
            call.ptype
          )
      }
    }
    val aggCalls  = calls.result()
    val outSchema = Schema(keys.map((n, e) => Field(n, e.ptype)) ++ fields.result())
    HashAggOp(input, keys.map(_._2), aggCalls, outSchema)

  end planPivot

  /** UNPIVOT: one row per (input row, target column) with a non-NULL value */
  private def planUnpivot(u: Unpivot): Operator =
    val child   = planRelation(u.child)
    val batches = Operator.materialize(child)
    val schema  = child.schema
    val key     = u.unpivotKey
    val targets = key
      .targetColumns
      .map { c =>
        val i = schema.names.indexWhere(_.equalsIgnoreCase(c.leafName))
        if i < 0 then
          throw ColumnNotFound(s"Column not found: ${c.leafName}")
        i
      }
    val others    = schema.fields.indices.filterNot(targets.contains)
    val valueType = targets.map(schema(_).ptype).reduce(PType.common)
    val outSchema = Schema(
      others.map(schema(_)).map(_.noQualifier) ++
        IndexedSeq(
          Field(key.unpivotColumnName.leafName, PType.Str),
          Field(key.getValueColumnName.leafName, valueType)
        )
    )
    val parts = targets.map { t =>
      val in    = BatchesOp(schema, Seq(batches))
      val value = castExpr(ColRef(t, schema(t).ptype, schema(t).name), valueType)
      val proj  = ProjectOp(
        FilterOp(in, IsNullExpr(ColRef(t, schema(t).ptype, schema(t).name), negate = true)),
        others.map(i => ColRef(i, schema(i).ptype, schema(i).name): VExpr) ++
          IndexedSeq(Const(schema(t).name, PType.Str), value),
        outSchema
      )
      proj: Operator
    }
    UnionAllOp(parts, outSchema)

  end planUnpivot

  private def planSample(s: Sample): Operator =
    val op = planRelation(s.child)
    s.size match
      case SamplingSize.Rows(n) =>
        LimitOp(op, n, 0)
      case SamplingSize.Percentage(p) =>
        val rnd = scala.util.Random(0)
        FilterOp(
          op,
          RowFn("sample", Nil, PType.Bool, _ => rnd.nextDouble() * 100 < p, nullSafe = true)
        )
      case other =>
        op

  // --- SQL SELECT blocks ----------------------------------------------------------------

  private def planSQLSelect(s: SQLSelect): Operator =
    var r: Relation = s.child
    s.filters.foreach(f => r = Filter(r, f.filterExpr, f.span))
    if s.groupingKeys.nonEmpty then
      r = GroupBy(r, s.groupingKeys, s.span)
      s.having.foreach(h => r = Filter(r, h.filterExpr, h.span))
    r = Project(r, s.selectItems, s.span)
    if s.orderBy.nonEmpty then
      r = Sort(r, s.orderBy.toList, s.span)
    s.limit
      .foreach {
        case l: LongLiteral =>
          r = Limit(r, l, s.span)
        case _ =>
      }
    planRelation(r)

  // --- projection -----------------------------------------------------------------------

  /** Output name of a select item */
  def nameOf(a: Attribute): String =
    if a.nameExpr.nonEmpty && !a.nameExpr.leafName.startsWith("<") then
      a.nameExpr.leafName
    else
      a match
        case s: SingleColumn =>
          exprName(s.expr)
        case al: Alias =>
          exprName(al.expr)
        case other =>
          exprName(other)

  def exprName(e: Expression): String =
    e match
      case i: Identifier =>
        i.leafName
      case d: DotRef =>
        d.name.leafName
      case t: TypedExpression =>
        exprName(t.child)
      case s: SingleColumn if s.nameExpr.nonEmpty =>
        s.nameExpr.leafName
      case s: SingleColumn =>
        exprName(s.expr)
      case other =>
        val sql =
          try
            SqlGenerator(
              CodeFormatterConfig(sqlDBType = DBType.DuckDB, fitToLine = true, maxLineWidth = 10000)
            ).print(other)
          catch
            case _: Exception =>
              other.toString
        sql.replace("count(*)", "count_star()").replaceAll("\\s+", " ")

  /** Bind select items over an operator, expanding `*` */
  private def bindItems(
      binder: Binder,
      op: Operator,
      items: List[Attribute]
  ): (IndexedSeq[VExpr], IndexedSeq[Field]) =
    val exprs  = IndexedSeq.newBuilder[VExpr]
    val fields = IndexedSeq.newBuilder[Field]
    items.foreach {
      case a: AllColumns =>
        val qualifier =
          a.nameExpr match
            case d: DotRef =>
              Some(d.qualifier).collect { case i: Identifier =>
                i.leafName
              }
            case _ =>
              None
        val visibleFields = binder.scope.schema.fields
        visibleFields
          .indices
          .foreach { i =>
            val f = visibleFields(i)
            if !isHidden(f) && qualifier.forall(q => f.qualifiers.exists(_.equalsIgnoreCase(q)))
            then
              exprs += ColRef(binder.scope.offset + i, f.ptype, f.name)
              fields += f
          }
      case attr =>
        val e = binder.bind(attr)
        exprs += e
        val qualifiers =
          e match
            case c: ColRef
                if c.index - binder.scope.offset >= 0 &&
                  c.index - binder.scope.offset < binder.scope.width =>
              binder.scope.schema(c.index - binder.scope.offset).qualifiers
            case _ =>
              Nil
        fields += Field(nameOf(attr), e.ptype, qualifiers)
    }
    (exprs.result(), fields.result())

  end bindItems

  private def planProject(child: Relation, items: List[Attribute]): Operator =
    if isAggregationInput(child) || items.exists(i => Binder.containsAggregate(i)) then
      planAggregation(child, Some(items), defaultItems = false)
    else if items.exists(i => unnestArg(i).isDefined) then
      projectWithUnnest(planRelation(child), items)
    else
      val op = planRelation(child)
      projectWithWindows(op, items)

  // --- unnest ---------------------------------------------------------------------------

  private def isUnnest(name: NameExpr): Boolean = name.leafName.equalsIgnoreCase("unnest")

  /** unnest(...) as a relation, with its alias */
  private def unnestCall(r: Relation): Option[(TableFunctionCall, Option[AliasedRelation])] =
    r match
      case t: TableFunctionCall if isUnnest(t.name) =>
        Some((t, None))
      case a @ AliasedRelation(t: TableFunctionCall, _, _, _) if isUnnest(t.name) =>
        Some((t, Some(a)))
      case _ =>
        None

  /** The list argument of a select item `unnest(list)` */
  private def unnestArg(e: Expression): Option[Expression] =
    e match
      case s: SingleColumn =>
        unnestArg(s.expr)
      case a: Alias =>
        unnestArg(a.expr)
      case t: TypedExpression =>
        unnestArg(t.child)
      case f: FunctionApply if f.window.isEmpty && Binder.functionName(f) == "unnest" =>
        f.args match
          case List(a) =>
            Some(a.value)
          case _ =>
            throw StatusCode.NOT_IMPLEMENTED.newException("unnest expects a single list argument")
      case _ =>
        None

  /** The element type of lists produced by a constant expression, or Any */
  private def elementType(e: VExpr): PType =
    if !e.isConstant && !e.isInstanceOf[Const] then
      PType.AnyT
    else
      e.eval(Batch.ofRows(1)).get(0) match
        case s: Seq[?] =>
          val ts =
            s.filter(_ != null)
              .map {
                case _: Int =>
                  PType.Int32
                case _: Long =>
                  PType.Int64
                case _: Double =>
                  PType.Float64
                case _: String =>
                  PType.Str
                case _: Boolean =>
                  PType.Bool
                case _ =>
                  PType.AnyT
              }
              .distinct
          ts match
            case Seq() =>
              PType.Int32
            case Seq(t) =>
              t
            case many if many.forall(_.isNumeric) =>
              many.reduce(PType.common)
            case _ =>
              PType.AnyT
        case _ =>
          PType.AnyT

  /** unnest(list, ...) as a relation; over `left` (a lateral join) when given */
  private def planUnnest(
      left: Option[Operator],
      call: TableFunctionCall,
      alias: Option[AliasedRelation]
  ): Operator =
    val input  = left.getOrElse(SingleRowOp())
    val binder = Binder(this, BindScope(input.schema))
    val lists  = call.args.map(a => binder.bind(a.value)).toIndexedSeq
    val names  = alias.flatMap(_.columnNames).map(_.map(_.name.name)).getOrElse(Nil)
    val q      = alias.map(_.alias.leafName).toList
    val fields = lists
      .indices
      .map { i =>
        Field(names.lift(i).getOrElse("unnest"), elementType(lists(i)), q)
      }
    val inputFields =
      if left.isDefined then
        input.schema.fields
      else
        IndexedSeq.empty
    UnnestOp(input, lists, keepInput = left.isDefined, Schema(inputFields ++ fields))

  /** select ..., unnest(list), ...: the row repeats per list element */
  private def projectWithUnnest(op: Operator, items: List[Attribute]): Operator =
    val replaced = items.map { i =>
      unnestArg(i) match
        case Some(arg) =>
          SingleColumn(NameExpr.fromString(nameOf(i)), arg, NoSpan)
        case None =>
          i
    }
    val binder          = Binder(this, BindScope(op.schema))
    val (exprs, fields) = bindItems(binder, op, replaced)
    val proj            = ProjectOp(op, exprs, Schema(fields))
    val listIdx         = items.indices.filter(i => unnestArg(items(i)).isDefined)
    val elemFields      = listIdx.map(i => fields(i).copy(ptype = elementType(exprs(i))))
    val unnest          = UnnestOp(
      proj,
      listIdx.map(i => ColRef(i, fields(i).ptype, fields(i).name): VExpr),
      keepInput = true,
      Schema(fields ++ elemFields)
    )
    val outExprs = items
      .indices
      .map { i =>
        listIdx.indexOf(i) match
          case -1 =>
            ColRef(i, fields(i).ptype, fields(i).name): VExpr
          case k =>
            ColRef(fields.size + k, elemFields(k).ptype, elemFields(k).name)
      }
    val outFields = items
      .indices
      .map { i =>
        listIdx.indexOf(i) match
          case -1 =>
            fields(i)
          case k =>
            elemFields(k)
      }
    ProjectOp(unnest, outExprs, Schema(outFields))

  end projectWithUnnest

  /** Bind a projection, evaluating window functions first when there are any */
  private def projectWithWindows(op: Operator, items: List[Attribute]): Operator =
    val (in, (exprs, fields)) = withWindows(op, items)((o, b) => bindItems(b, o, items))
    ProjectOp(in, exprs, Schema(fields))

  /**
    * Bind expressions over `op`. When they contain window functions, a window operator is placed
    * over `op` (input columns first, then the window results), and binding runs against it
    *
    * @return
    *   the operator to evaluate the bound result over, and the result of `bindAll`
    */
  private def withWindows[A](op: Operator, items: List[Expression])(
      bindAll: (Operator, Binder) => A
  ): (Operator, A) =
    if items.exists(i => Binder.containsWindow(i)) then
      val collector = WindowCollector(op)
      val saved     = windowCollector
      windowCollector = Some(collector)
      try
        // First pass registers window calls; the window operator appends their results
        bindAll(op, Binder(this, BindScope(op.schema)))
        val winOp = collector.build()
        windowCollector = Some(collector.frozen(winOp))
        (winOp, bindAll(winOp, Binder(this, BindScope(winOp.schema))))
      finally
        windowCollector = saved
    else
      (op, bindAll(op, Binder(this, BindScope(op.schema))))

  private def isAggregationInput(r: Relation): Boolean =
    r match
      case _: GroupBy =>
        true
      case Filter(_: GroupBy, _, _) =>
        true
      case _ =>
        false

  private def planColumnEdit(
      child: Relation,
      prepend: Boolean,
      newColumns: List[Attribute]
  ): Operator =
    val input = planRelation(child)
    // Window functions in new columns are evaluated first; their results follow the input columns
    val (op, (exprs, fields)) =
      withWindows(input, newColumns) { (op, binder) =>
        bindItems(binder, op, newColumns)
      }
    // A new column with an existing name replaces it in place
    val existing = input
      .schema
      .fields
      .indices
      .map { i =>
        val f = input.schema(i)
        val k = fields.indexWhere(_.name.equalsIgnoreCase(f.name))
        if k >= 0 then
          (exprs(k), fields(k))
        else
          (ColRef(i, f.ptype, f.name), f)
      }
    val added = fields
      .indices
      .filterNot(k => input.schema.fields.exists(_.name.equalsIgnoreCase(fields(k).name)))
      .map(k => (exprs(k), fields(k)))
    val all =
      if prepend then
        added ++ existing
      else
        existing ++ added
    ProjectOp(op, all.map(_._1), Schema(all.map(_._2)))

  end planColumnEdit

  // --- aggregation ----------------------------------------------------------------------

  private def planAggregation(
      input: Relation,
      items: Option[List[Attribute]],
      defaultItems: Boolean
  ): Operator =
    val (groupBy, having) =
      input match
        case g: GroupBy =>
          (Some(g), None)
        case Filter(g: GroupBy, cond, _) =>
          (Some(g), Some(cond))
        case _ =>
          (None, None)
    val source                     = groupBy.map(_.child).getOrElse(input)
    val inOp                       = planRelation(source)
    val scope                      = BindScope(inOp.schema)
    val binder                     = Binder(this, scope)
    val keyDefs: List[GroupingKey] = groupBy.map(_.groupingKeys).getOrElse(Nil)
    // Grouping keys; `group by 1` and `group by <select alias>` refer to select items
    val keyExprs =
      keyDefs
        .map { k =>
          def selectItem(i: Int): Option[Attribute] = items.flatMap(_.lift(i))
          val e: Expression                         =
            k.child match
              case l: LongLiteral if items.isDefined =>
                selectItem(l.value.toInt - 1).getOrElse(k.child)
              case other =>
                other
          val bound =
            try
              binder.bind(e)
            catch
              case ex: ColumnNotFound =>
                e match
                  case i: Identifier =>
                    items
                      .flatMap(_.find(a => nameOf(a).equalsIgnoreCase(i.leafName)))
                      .map(a => binder.bind(a))
                      .getOrElse(throw ex)
                  case _ =>
                    throw ex
          val name =
            if k.name.nonEmpty && !k.name.leafName.startsWith("<") then
              k.name.leafName
            else
              e match
                case a: Attribute =>
                  nameOf(a)
                case other =>
                  exprName(other)
          (name, bound)
        }
        .toIndexedSeq
    val aggCtx                       = AggContext(scope, keyExprs.map(_._1), keyExprs.map(_._2))
    val aggBinder                    = Binder(this, BindScope(aggCtx.outputSchema), Some(aggCtx))
    val selectItems: List[Attribute] = items.getOrElse {
      // GroupBy without a selection: keys, then an arbitrary value of every input column
      val keyItems =
        keyExprs
          .indices
          .map(i =>
            SingleColumn(
              NameExpr.fromString(keyExprs(i)._1),
              UnquotedIdentifier(keyExprs(i)._1, NoSpan),
              NoSpan
            )
          )
          .toList
      keyItems
    }
    // Bind select items in aggregation mode
    val boundItems = mutable.ArrayBuffer.empty[(VExpr, Field)]
    selectItems.foreach {
      case a: AllColumns =>
        // `select *` over a grouping: every key
        keyExprs
          .indices
          .foreach(i =>
            boundItems += ((aggCtx.keyRef(i), Field(keyExprs(i)._1, keyExprs(i)._2.ptype)))
          )
      case attr =>
        val e = aggBinder.bind(attr)
        boundItems += ((e, Field(nameOf(attr), e.ptype)))
    }
    if defaultItems && items.isEmpty then
      // arbitrary(c) for every input column
      inOp
        .schema
        .fields
        .indices
        .foreach { i =>
          val f = inOp.schema(i)
          if !isHidden(f) then
            val c = ColRef(i, f.ptype, f.name)
            val e = aggCtx.addCall(
              AggCall("arbitrary", Seq(c), distinct = false, filter = None, f.ptype)
            )
            boundItems += ((e, Field(s"arbitrary(${f.name})", f.ptype)))
        }
    val havingExpr      = having.map(h => aggBinder.bind(h))
    val aggOp: Operator =
      val keys  = keyExprs.map(_._2)
      val calls = aggCtx.calls.toIndexedSeq
      HashAggOp(inOp, keys, calls, aggCtx.outputSchema)
    val filtered = havingExpr.map(h => FilterOp(aggOp, h)).getOrElse(aggOp)
    val items2   = boundItems.toIndexedSeq
    if selectItems.exists(a => Binder.containsWindow(a)) then
      // Window functions over aggregated rows: project the aggregate columns first
      val pre = ProjectOp(
        filtered,
        (0 until filtered.schema.size).map(i =>
          ColRef(i, filtered.schema(i).ptype, filtered.schema(i).name)
        ),
        filtered.schema
      )
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException("Window functions over aggregations are not supported yet")
    else
      ProjectOp(filtered, items2.map(_._1), Schema(items2.map(_._2)))

  end planAggregation

  // --- sort -----------------------------------------------------------------------------

  private def sortKeys(binder: Binder, items: List[SortItem], schema: Schema): Seq[SortKey] = items
    .map { s =>
      val e =
        s.sortKey match
          case l: LongLiteral =>
            val i = l.value.toInt - 1
            ColRef(binder.scope.offset + i, schema(i).ptype, schema(i).name)
          case other =>
            binder.bind(other)
      val asc = !s.ordering.contains(SortOrdering.Descending)
      // DuckDB's default: NULLS LAST for both directions
      val nullsFirst =
        s.nullOrdering match
          case Some(NullOrdering.NullIsFirst) =>
            true
          case Some(NullOrdering.NullIsLast) =>
            false
          case _ =>
            false
      SortKey(e, asc, nullsFirst)
    }

  private def planSort(s: Sort, limit: Option[Long]): Operator =
    val op = planRelation(s.child)
    try
      val keys = sortKeys(Binder(this, BindScope(op.schema)), s.orderBy, op.schema)
      SortOp(op, keys, limit)
    catch
      case e: ColumnNotFound =>
        // ORDER BY a column of the projection input: carry it as a hidden column
        s.child match
          case p: Project
              if !isAggregationInput(p.child) &&
                !p.selectItems.exists(i => Binder.containsAggregate(i)) =>
            val in              = planRelation(p.child)
            val binder          = Binder(this, BindScope(in.schema))
            val (exprs, fields) = bindItems(binder, in, p.selectItems)
            val keyExprs        = s.orderBy.map(k => binder.bind(k.sortKey))
            val hidden = keyExprs.indices.map(i => Field(hiddenName("sort"), keyExprs(i).ptype))
            val proj   = ProjectOp(in, exprs ++ keyExprs, Schema(fields ++ hidden))
            val keys   = sortKeys(Binder(this, BindScope(proj.schema)), s.orderBy, proj.schema)
            val keys2  = keys
              .zipWithIndex
              .map { (k, i) =>
                k.copy(expr = ColRef(exprs.size + i, keyExprs(i).ptype, hidden(i).name))
              }
            dropHidden(SortOp(proj, keys2, limit))
          case _ =>
            throw e

  // --- filters and subqueries -----------------------------------------------------------

  private def conjuncts(e: Expression): List[Expression] = splitAnd(Binder.expandTemplates(e))
    .flatMap(factorOr)

  private def splitOr(e: Expression): List[Expression] =
    e match
      case o: Or =>
        splitOr(o.left) ++ splitOr(o.right)
      case p: ParenthesizedExpression =>
        splitOr(p.child)
      case other =>
        List(other)

  private def exprKey(e: Expression): String = exprName(e)

  /** (a and b) or (a and c) => a and (b or c), so join keys inside OR branches become usable */
  private def factorOr(e: Expression): List[Expression] =
    e match
      case o: Or =>
        val branches = splitOr(o).map(splitAnd)
        val keySets  = branches.map(_.map(exprKey).toSet)
        val common   = keySets.reduce(_ intersect _)
        if common.isEmpty then
          List(e)
        else
          val commonExprs = branches.head.filter(c => common.contains(exprKey(c)))
          val rest        = branches.map(_.filterNot(c => common.contains(exprKey(c))))
          if rest.exists(_.isEmpty) then
            commonExprs
          else
            commonExprs :+
              rest.map(_.reduce((a, b) => And(a, b, NoSpan))).reduce((a, b) => Or(a, b, NoSpan))
      case other =>
        List(other)

  private def splitAnd(e: Expression): List[Expression] =
    e match
      case a: And =>
        splitAnd(a.left) ++ splitAnd(a.right)
      case p: ParenthesizedExpression =>
        splitAnd(p.child)
      case other =>
        List(other)

  private def planFilter(f: Filter): Operator =
    f.child match
      case g: GroupBy =>
        // HAVING without a selection
        planAggregation(f, None, defaultItems = true)
      case j: Join if isInnerJoin(j) =>
        planJoinTree(j, conjuncts(f.filterExpr))
      case child =>
        applyConjuncts(planRelation(child), conjuncts(f.filterExpr))

  /** Apply filter conjuncts, turning subqueries into joins where possible */
  private def applyConjuncts(input: Operator, conds: List[Expression]): Operator =
    val originalWidth = input.schema.size
    var op            = input
    val plain         = mutable.ArrayBuffer.empty[Expression]
    conds.foreach { c =>
      if sys.env.contains("ENGINE_DEBUG") then
        warn(s"conjunct: ${c.getClass.getSimpleName}: ${c}")
      rewriteSubqueryConjunct(op, c) match
        case Some((newOp, residual)) =>
          op = newOp
          residual.foreach(plain += _)
        case None =>
          plain += c
    }
    if plain.nonEmpty then
      // Subquery joins may extend the input, so bind every condition after all joins are added
      val bound = mutable.ArrayBuffer.empty[(Expression, VExpr)]
      plain.foreach { c =>
        val (newOp, e) = bindWithSubqueries(op, c)
        if newOp ne op then
          // Earlier conditions were bound to the narrower input, whose columns keep their positions
          op = newOp
        bound += ((c, e))
      }
      val cond = bound.map(_._2).reduce((a, b) => AndExpr(a, b))
      op = FilterOp(op, cond)
    if op.schema.size != originalWidth then
      projectIndexes(op, 0 until originalWidth)
    else
      op

  end applyConjuncts

  /**
    * Bind one conjunct whose correlated scalar subqueries become left joins; returns the (possibly
    * extended) input and the bound condition
    */
  private def bindWithSubqueries(input: Operator, cond: Expression): (Operator, VExpr) =
    var op                                 = input
    def rewrite(e: Expression): Expression =
      e match
        case s: SubQueryExpression if isCorrelated(s.query, op.schema) =>
          val (joined, column) = decorrelateScalar(op, s.query)
          op = joined
          UnquotedIdentifier(column, NoSpan)
        case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
          e
        case other =>
          other.transformChildExpressions { case c: Expression =>
            rewrite(c)
          }
    val rewritten = rewrite(cond)
    (op, Binder(this, BindScope(op.schema)).bind(rewritten))

  /** Whether a subquery references columns of the enclosing schema */
  private def isCorrelated(sub: Relation, outer: Schema): Boolean =
    try
      Planner.nested(this).planRelation(sub)
      false
    catch
      case e: ColumnNotFound =>
        true

  /**
    * EXISTS / NOT EXISTS / IN / NOT IN conjuncts as semi and anti joins. Returns the new input and
    * any condition left to evaluate
    */
  private def rewriteSubqueryConjunct(
      op: Operator,
      c: Expression
  ): Option[(Operator, Option[Expression])] =
    c match
      case e: Exists =>
        existsJoin(op, e, negate = false)
      case Not(e: Exists, _) =>
        existsJoin(op, e, negate = true)
      case in: InRelation =>
        inJoin(op, in.a, in.in, negate = false)
      case in: NotInRelation =>
        inJoin(op, in.a, in.in, negate = true)
      case Not(in: InRelation, _) =>
        inJoin(op, in.a, in.in, negate = true)
      case _ =>
        None

  private def subqueryRelation(e: Expression): Relation =
    e match
      case s: SubQueryExpression =>
        s.query
      case p: ParenthesizedExpression =>
        subqueryRelation(p.child)
      case r: Relation =>
        r
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported EXISTS argument: ${other}")

  /** Peel wrappers that don't change the rows of a subquery */
  private def stripWrappers(r: Relation): Relation =
    r match
      case q: Query =>
        stripWrappers(q.body)
      case b: BracedRelation =>
        stripWrappers(b.child)
      case s: SelectAsAlias =>
        stripWrappers(s.child)
      case other =>
        other

  /**
    * Plan a correlated subquery body `[Project] Filter(child, cond)`: conjuncts referencing only
    * the child stay as a filter; the rest are returned as correlated conditions
    */
  private def planCorrelated(
      sub: Relation,
      outer: Schema
  ): (Operator, List[Expression], Option[List[Attribute]]) =
    val (items, body) =
      stripWrappers(sub) match
        case p: Project =>
          (Some(p.selectItems), stripWrappers(p.child))
        case d: Distinct =>
          (Some(d.child.selectItems), stripWrappers(d.child.child))
        case other =>
          (None, other)
    body match
      case f: Filter =>
        // Planning is lazy, so planning the child only to learn its schema is cheap
        val childSchema         = planRelation(f.child).schema
        val (local, correlated) = conjuncts(f.filterExpr).partition(c => bindsIn(c, childSchema))
        val filtered            =
          if local.isEmpty then
            planRelation(f.child)
          else
            planFilter(Filter(f.child, local.reduce((a, b) => And(a, b, NoSpan)), f.span))
        (filtered, correlated, items)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported correlated subquery shape: ${other.nodeName}")

  private def bindsIn(e: Expression, schema: Schema): Boolean =
    try
      Binder(Planner.nested(this), BindScope(schema)).bind(e)
      true
    catch
      case _: ColumnNotFound =>
        false

  /** Split correlated conditions into (inner key, outer key) equalities and the rest */
  private def correlationKeys(
      conds: List[Expression],
      inner: Schema,
      outer: Schema
  ): (List[(Expression, Expression)], List[Expression]) =
    val keys  = List.newBuilder[(Expression, Expression)]
    val other = List.newBuilder[Expression]
    conds.foreach {
      case eq: Eq
          if bindsIn(eq.left, inner) && !bindsIn(eq.right, inner) && bindsIn(eq.right, outer) =>
        keys += ((eq.left, eq.right))
      case eq: Eq
          if bindsIn(eq.right, inner) && !bindsIn(eq.left, inner) && bindsIn(eq.left, outer) =>
        keys += ((eq.right, eq.left))
      case c =>
        other += c
    }
    (keys.result(), other.result())

  /** Equi-join keys bound on both sides and cast to common types */
  private def joinKeys(
      lb: Binder,
      rb: Binder,
      pairs: Seq[(Expression, Expression)]
  ): (IndexedSeq[VExpr], IndexedSeq[VExpr]) =
    val bound = pairs.map { (l, r) =>
      val le = lb.bind(l)
      val re = rb.bind(r)
      val t  = PType.common(le.ptype, re.ptype)
      (castExpr(le, t), castExpr(re, t))
    }
    (bound.map(_._1).toIndexedSeq, bound.map(_._2).toIndexedSeq)

  private def castExpr(e: VExpr, t: PType): VExpr =
    if e.ptype == t then
      e
    else
      CastExpr(e, t)

  /** Residual condition over (outer ++ inner) with inner-first name resolution */
  private def residualBinder(outer: Schema, inner: Schema): Binder = Binder(
    this,
    BindScope(inner, offset = outer.size, parent = Some(BindScope(outer)))
  )

  private def existsJoin(
      op: Operator,
      e: Exists,
      negate: Boolean
  ): Option[(Operator, Option[Expression])] =
    val sub = subqueryRelation(e.child)
    if !isCorrelated(sub, op.schema) then
      // Uncorrelated: a constant condition
      val rows = Operator.materialize(Planner.nested(this).planRelation(sub)).size
      val keep = (rows > 0) != negate
      Some(
        (
          if keep then
            op
          else
            FilterOp(op, Const(false, PType.Bool))
          ,
          None
        )
      )
    else
      val (inner, correlated, _) = planCorrelated(sub, op.schema)
      val (pairs, rest)          = correlationKeys(correlated, inner.schema, op.schema)
      val kind                   =
        if negate then
          JoinKind.Anti
        else
          JoinKind.Semi
      val residual = Option
        .when(rest.nonEmpty)(rest.reduce((a, b) => And(a, b, NoSpan)))
        .map(r => residualBinder(op.schema, inner.schema).bind(r))
      if pairs.isEmpty then
        Some((NestedLoopJoinOp(op, inner, residual, kind, op.schema), None))
      else
        val (lk, rk) = joinKeys(
          Binder(this, BindScope(pairs.map(_._2).headOption.map(_ => op.schema).get)),
          Binder(this, BindScope(inner.schema)),
          pairs.map(p => (p._2, p._1))
        )
        Some((HashJoinOp(op, inner, lk, rk, kind, residual, op.schema), None))
    end if

  end existsJoin

  private def inJoin(
      op: Operator,
      a: Expression,
      sub: Relation,
      negate: Boolean
  ): Option[(Operator, Option[Expression])] =
    val correlated                = isCorrelated(sub, op.schema)
    val (inner, corrConds, items) =
      if correlated then
        planCorrelated(sub, op.schema)
      else
        (Planner.nested(this).planRelation(sub), Nil, None)
    // The compared value: the single select item
    val (innerOp, valueIdx) =
      items match
        case Some(its) =>
          val b               = Binder(this, BindScope(inner.schema))
          val (exprs, fields) = bindItems(b, inner, its)
          val proj            = ProjectOp(
            inner,
            inner
              .schema
              .fields
              .indices
              .map(i => ColRef(i, inner.schema(i).ptype, inner.schema(i).name)) ++ exprs,
            Schema(inner.schema.fields ++ fields.map(f => f.withName(hiddenName("in"))))
          )
          (proj, inner.schema.size)
        case None =>
          (inner, 0)
    val (pairs, rest) = correlationKeys(corrConds, inner.schema, op.schema)
    val lb            = Binder(this, BindScope(op.schema))
    val target        = lb.bind(a)
    val valueRef   = ColRef(valueIdx, innerOp.schema(valueIdx).ptype, innerOp.schema(valueIdx).name)
    val t          = PType.common(target.ptype, valueRef.ptype)
    val (lk0, rk0) = joinKeys(
      lb,
      Binder(this, BindScope(inner.schema)),
      pairs.map(p => (p._2, p._1))
    )
    val lk       = castExpr(target, t) +: lk0
    val rk       = castExpr(valueRef, t) +: rk0
    val residual = Option
      .when(rest.nonEmpty)(rest.reduce((x, y) => And(x, y, NoSpan)))
      .map(r => residualBinder(op.schema, innerOp.schema).bind(r))
    if !negate then
      Some((HashJoinOp(op, innerOp, lk, rk, JoinKind.Semi, residual, op.schema), None))
    else if !correlated then
      // NOT IN: no row passes when the subquery has a NULL; NULL targets never pass
      val values  = Operator.materialize(innerOp)
      val v       = values.columns(valueIdx)
      val hasNull = (0 until values.size).exists(v.isNull)
      if hasNull then
        Some((FilterOp(op, Const(false, PType.Bool)), None))
      else
        val src  = BatchesOp(innerOp.schema, Seq(values))
        val anti = HashJoinOp(op, src, lk, rk, JoinKind.Anti, residual, op.schema)
        Some((FilterOp(anti, IsNullExpr(target, negate = true)), None))
    else
      Some((HashJoinOp(op, innerOp, lk, rk, JoinKind.Anti, residual, op.schema), None))

  end inJoin

  /**
    * A correlated scalar subquery `[Project(agg)] Filter(child, k_inner = k_outer and ...)` as an
    * aggregation grouped by the inner keys, left-joined to the outer input. Returns the joined
    * operator and the name of the column holding the subquery value
    */
  private def decorrelateScalar(outer: Operator, sub: Relation): (Operator, String) =
    val (inner, correlated, items) = planCorrelated(sub, outer.schema)
    val (pairs, rest)              = correlationKeys(correlated, inner.schema, outer.schema)
    if rest.nonEmpty || pairs.isEmpty then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"Unsupported correlated scalar subquery: only equality correlations are supported: ${rest
              .map(_.toString)
              .mkString(", ")} (outer: ${outer.schema.names.mkString(",")}; inner: ${inner
              .schema
              .names
              .mkString(",")})"
        )
    val innerScope = BindScope(inner.schema)
    val ib         = Binder(this, innerScope)
    val ob         = Binder(this, BindScope(outer.schema))
    val keyNames   = pairs.indices.map(_ => hiddenName("k"))
    val innerKeys  = pairs.map(p => ib.bind(p._1)).toIndexedSeq
    val aggCtx     = AggContext(innerScope, keyNames, innerKeys)
    val item       = items
      .flatMap(_.headOption)
      .getOrElse {
        throw StatusCode.NOT_IMPLEMENTED.newException("A scalar subquery needs a select item")
      }
    val value     = Binder(this, BindScope(aggCtx.outputSchema), Some(aggCtx)).bind(item)
    val agg       = HashAggOp(inner, innerKeys, aggCtx.calls.toIndexedSeq, aggCtx.outputSchema)
    val valueName = hiddenName("sq")
    val proj      = ProjectOp(
      agg,
      innerKeys.indices.map(i => aggCtx.keyRef(i)).toIndexedSeq :+ value,
      Schema(
        keyNames.indices.map(i => Field(keyNames(i), innerKeys(i).ptype)).toIndexedSeq :+
          Field(valueName, value.ptype)
      )
    )
    val outerKeys = pairs.map(p => ob.bind(p._2)).toIndexedSeq
    val lk = outerKeys.zip(innerKeys).map((o, i) => castExpr(o, PType.common(o.ptype, i.ptype)))
    val rk = innerKeys
      .indices
      .map(i => castExpr(aggCtx.keyRef(i), PType.common(outerKeys(i).ptype, innerKeys(i).ptype)))
    val joined = HashJoinOp(outer, proj, lk, rk, JoinKind.Left, None, outer.schema ++ proj.schema)
    (joined, valueName)

  end decorrelateScalar

  /** Uncorrelated scalar subquery as a constant */
  def bindScalarSubquery(binder: Binder, sub: Relation): VExpr =
    val op = Planner.nested(this).planRelation(sub)
    val b  = Operator.materialize(op)
    if b.size > 1 then
      throw StatusCode.INVALID_ARGUMENT.newException("A scalar subquery returned more than one row")
    val t = op.schema.fields.headOption.map(_.ptype).getOrElse(PType.Null)
    val v =
      if b.size == 0 || b.numColumns == 0 then
        null
      else
        b.columns(0).get(0)
    Const(v, t)

  /** IN (subquery) in a non-filter position: an uncorrelated value set */
  def bindInSubquery(binder: Binder, a: Expression, sub: Relation, negate: Boolean): VExpr =
    val op     = Planner.nested(this).planRelation(sub)
    val b      = Operator.materialize(op)
    val target = binder.bind(a)
    val t      = PType.common(target.ptype, op.schema(0).ptype)
    val col    = Kernels.castTo(b.columns(0), t)
    val values = (0 until b.size).map(col.get)
    InList(castExpr(target, t), values.filter(_ != null).toSet, values.contains(null), negate)

  /** (a, b) [NOT] IN (subquery) over an uncorrelated subquery, with SQL NULL semantics */
  def bindTupleInSubquery(
      binder: Binder,
      tuple: List[Expression],
      sub: Relation,
      negate: Boolean
  ): VExpr =
    val op = Planner.nested(this).planRelation(sub)
    if op.schema.size != tuple.size then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"Tuple size mismatch: ${tuple.size} vs ${op.schema.size} subquery columns")
    val b       = Operator.materialize(op)
    val targets = tuple.map(binder.bind)
    val types   = targets.indices.map(i => PType.common(targets(i).ptype, op.schema(i).ptype))
    val cols    = types.indices.map(i => Kernels.castTo(b.columns(i), types(i)))
    val rows    = (0 until b.size).map(r => cols.map(_.get(r)))
    val exact   = rows.filter(_.forall(_ != null)).toSet
    RowFn(
      if negate then
        "tuple_not_in"
      else
        "tuple_in"
      ,
      targets.indices.map(i => castExpr(targets(i), types(i))),
      PType.Bool,
      a =>
        val t       = a.toIndexedSeq
        val in: Any =
          if t.forall(_ != null) && exact.contains(t) then
            true
          else
            // NULL when some row could match except for NULLs
            val unknown = rows.exists(r =>
              r.indices.forall(i => r(i) == null || t(i) == null || r(i) == t(i))
            )
            if unknown then
              null
            else
              false
        in match
          case b: Boolean if negate =>
            !b
          case other =>
            other
      ,
      nullSafe = true
    )

  end bindTupleInSubquery

  def bindExists(binder: Binder, e: Exists): VExpr =
    val op = Planner.nested(this).planRelation(subqueryRelation(e.child))
    Const(op.next() != null, PType.Bool)

  def windowRef(binder: Binder, e: Expression): VExpr =
    windowCollector match
      case Some(c) =>
        c.ref(binder, e)
      case None =>
        throw StatusCode.NOT_IMPLEMENTED.newException("Window functions are not supported here")

  // --- joins ----------------------------------------------------------------------------

  private def isInnerJoin(j: Join): Boolean =
    !j.asof && (
      j.joinType match
        case JoinType.InnerJoin | JoinType.ImplicitJoin | JoinType.CrossJoin =>
          j.cond match
            case _: JoinOnTheSameColumns | _: NaturalJoin =>
              false
            case _ =>
              true
        case _ =>
          false
    )

  /** Leaves and conditions of a tree of inner joins */
  private def flattenJoins(r: Relation): (List[Relation], List[Expression]) =
    r match
      case j: Join if isInnerJoin(j) =>
        val (l, lc)  = flattenJoins(j.left)
        val (rr, rc) = flattenJoins(j.right)
        val cond     =
          j.cond match
            case JoinOn(e, _) =>
              conjuncts(e)
            case JoinOnEq(keys, _) =>
              conjuncts(Expression.concatWithEq(keys))
            case _ =>
              Nil
        (l ++ rr, lc ++ rc ++ cond)
      case other =>
        (List(other), Nil)

  private case class JoinInput(op: Operator, rows: Double)

  /**
    * Plan inner joins and filter conditions together: single-input conditions are pushed to their
    * inputs, equalities between inputs become hash join keys, and inputs are joined greedily,
    * building on the smaller side
    */
  private def planJoinTree(j: Join, filters: List[Expression]): Operator =
    unnestCall(j.right) match
      case Some((call, alias)) =>
        // A lateral unnest over the left input: join ... unnest(left.column)
        val op    = planUnnest(Some(planRelation(j.left)), call, alias)
        val conds =
          j.cond match
            case JoinOn(e, _) =>
              conjuncts(e)
            case JoinOnEq(keys, _) =>
              conjuncts(Expression.concatWithEq(keys))
            case _ =>
              Nil
        return applyConjuncts(op, (conds ++ filters).filterNot(isTrueLiteral))
      case None =>
    if !isInnerJoin(j) then
      return applyConjuncts(planOuterJoin(j), filters)
    val (leaves, joinConds) = flattenJoins(j)
    val inputs              = leaves.map(planRelation)
    val all                 = (joinConds ++ filters).filterNot(isTrueLiteral)
    // Conditions with subqueries are applied after joining
    val (subqueryConds, conds) = all.partition(hasSubquery)
    // Which inputs a condition references
    def refs(c: Expression): Set[Int] =
      inputs.indices.filter(i => referencesInput(c, inputs, i)).toSet
    val condRefs = conds.map(c => (c, refs(c)))
    // Push single-input conditions down
    val pushed = inputs
      .indices
      .map { i =>
        val local = condRefs.collect {
          case (c, r) if r == Set(i) =>
            c
        }
        val op =
          if local.isEmpty then
            inputs(i)
          else
            applyConjuncts(inputs(i), local)
        JoinInput(op, estimateRows(op) * math.pow(0.3, local.size))
      }
    var remaining = condRefs.filter(_._2.size != 1).map(_._1)
    // constant conditions (no references) apply at the end
    val joined = mutable.Set.empty[Int]
    // Start from the largest input as the probe side
    val start             = pushed.indices.maxBy(i => pushed(i).rows)
    var current: Operator = pushed(start).op
    var currentRows       = pushed(start).rows
    joined += start
    while joined.size < pushed.size do
      // Candidates connected by an equality condition
      val candidates = pushed
        .indices
        .filterNot(joined.contains)
        .map { i =>
          val eqs = remaining.filter(c => isEquiBetween(c, current.schema, pushed(i).op.schema))
          (i, eqs)
        }
      val connected   = candidates.filter(_._2.nonEmpty)
      val (next, eqs) =
        if connected.nonEmpty then
          connected.minBy(c => pushed(c._1).rows)
        else
          candidates.minBy(c => pushed(c._1).rows)
      val right    = pushed(next).op
      val combined = current.schema ++ right.schema
      if eqs.nonEmpty then
        val pairs    = eqs.map(c => equiSides(c, current.schema, right.schema))
        val (lk, rk) = joinKeys(
          Binder(this, BindScope(current.schema)),
          Binder(this, BindScope(right.schema)),
          pairs
        )
        current = HashJoinOp(current, right, lk, rk, JoinKind.Inner, None, combined)
        currentRows = math.max(currentRows, pushed(next).rows)
      else
        current = NestedLoopJoinOp(current, right, None, JoinKind.Cross, combined)
        currentRows = currentRows * pushed(next).rows
      remaining = remaining.filterNot(eqs.contains)
      joined += next
      // Apply conditions whose inputs are now all joined
      val ready = remaining.filter(c => bindsIn(c, current.schema))
      if ready.nonEmpty then
        current = FilterOp(
          current,
          Binder(this, BindScope(current.schema)).bind(ready.reduce((a, b) => And(a, b, NoSpan)))
        )
        remaining = remaining.filterNot(ready.contains)
    end while
    if remaining.nonEmpty then
      current = applyConjuncts(current, remaining)
    if subqueryConds.nonEmpty then
      current = applyConjuncts(current, subqueryConds)
    current

  end planJoinTree

  private def isTrueLiteral(e: Expression): Boolean = e.isInstanceOf[TrueLiteral]

  private def hasSubquery(e: Expression): Boolean =
    var found = false
    e.traverseExpressions {
      case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
        found = true
    }
    found

  private def referencesInput(c: Expression, inputs: List[Operator], i: Int): Boolean =
    // A condition references input i when it does not bind without it
    val others = inputs.indices.filter(_ != i).map(inputs(_).schema).foldLeft(Schema.empty)(_ ++ _)
    !bindsIn(c, others) && bindsIn(c, inputs(i).schema ++ others)

  private def isEquiBetween(c: Expression, left: Schema, right: Schema): Boolean =
    c match
      case eq: Eq =>
        (
          bindsIn(eq.left, left) && !bindsIn(eq.left, right) && bindsIn(eq.right, right) &&
            !bindsIn(eq.right, left)
        ) || (
          bindsIn(eq.right, left) && !bindsIn(eq.right, right) && bindsIn(eq.left, right) &&
            !bindsIn(eq.left, left)
        )
      case _ =>
        false

  private def equiSides(c: Expression, left: Schema, right: Schema): (Expression, Expression) =
    c match
      case eq: Eq if bindsIn(eq.left, left) && bindsIn(eq.right, right) =>
        (eq.left, eq.right)
      case eq: Eq =>
        (eq.right, eq.left)
      case other =>
        throw IllegalStateException(s"Not an equality: ${other}")

  private def estimateRows(op: Operator): Double =
    op match
      case s: ScanOp =>
        s.source.estimatedRows.map(_.toDouble).getOrElse(1e6)
      case b: BatchesOp =>
        1e3
      case other =>
        other.children.headOption.map(estimateRows).getOrElse(1e6)

  private def planOuterJoin(j: Join): Operator =
    val left  = planRelation(j.left)
    val right = planRelation(j.right)
    val kind  =
      j.joinType match
        case JoinType.LeftOuterJoin =>
          JoinKind.Left
        case JoinType.RightOuterJoin =>
          JoinKind.Right
        case JoinType.FullOuterJoin =>
          JoinKind.Full
        case JoinType.CrossJoin =>
          JoinKind.Cross
        case _ =>
          JoinKind.Inner
    val combined = left.schema ++ right.schema
    if j.asof then
      return planAsOfJoin(j, left, right, kind)
    j.cond match
      case u: JoinOnTheSameColumns =>
        val names = u.columns.map(_.leafName)
        usingJoin(left, right, names, kind)
      case _: NaturalJoin =>
        val names = left.schema.names.filter(n => right.schema.names.exists(_.equalsIgnoreCase(n)))
        usingJoin(left, right, names.toList, kind)
      case NoJoinCriteria =>
        NestedLoopJoinOp(
          left,
          right,
          None,
          if kind == JoinKind.Inner then
            JoinKind.Cross
          else
            kind
          ,
          combined
        )
      case other =>
        val condExpr =
          other match
            case JoinOn(e, _) =>
              e
            case JoinOnEq(keys, _) =>
              Expression.concatWithEq(keys)
            case x =>
              throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported join condition: ${x}")
        val conds       = conjuncts(condExpr)
        val (eqs, rest) = conds.partition(c => isEquiBetween(c, left.schema, right.schema))
        val residual    = Option
          .when(rest.nonEmpty)(rest.reduce((a, b) => And(a, b, NoSpan)))
          .map(r => Binder(this, BindScope(combined)).bind(r))
        if eqs.isEmpty then
          NestedLoopJoinOp(left, right, residual, kind, combined)
        else
          val pairs    = eqs.map(c => equiSides(c, left.schema, right.schema))
          val (lk, rk) = joinKeys(
            Binder(this, BindScope(left.schema)),
            Binder(this, BindScope(right.schema)),
            pairs
          )
          HashJoinOp(left, right, lk, rk, kind, residual, combined)
    end match

  end planOuterJoin

  /** JOIN ... USING (cols): one output column per key (coalesced for outer joins) */
  private def usingJoin(
      left: Operator,
      right: Operator,
      names: List[String],
      kind: JoinKind
  ): Operator =
    val lb    = Binder(this, BindScope(left.schema))
    val rb    = Binder(this, BindScope(right.schema))
    val pairs = names.map(n =>
      (UnquotedIdentifier(n, NoSpan): Expression, UnquotedIdentifier(n, NoSpan): Expression)
    )
    val (lk, rk) = joinKeys(lb, rb, pairs)
    val combined = left.schema ++ right.schema
    val join     = HashJoinOp(left, right, lk, rk, kind, None, combined)
    val lw       = left.schema.size
    val keyIdx   = names.map { n =>
      (
        left.schema.names.indexWhere(_.equalsIgnoreCase(n)),
        right.schema.names.indexWhere(_.equalsIgnoreCase(n))
      )
    }
    val keyExprs = keyIdx.map { (li, ri) =>
      val lf = left.schema(li)
      val rf = right.schema(ri)
      val t  = PType.common(lf.ptype, rf.ptype)
      kind match
        case JoinKind.Right =>
          castExpr(ColRef(lw + ri, rf.ptype, rf.name), t)
        case JoinKind.Full =>
          Coalesce(Seq(ColRef(li, lf.ptype, lf.name), ColRef(lw + ri, rf.ptype, rf.name)), t)
        case _ =>
          castExpr(ColRef(li, lf.ptype, lf.name), t)
    }
    val keyFields = keyIdx
      .zip(keyExprs)
      .map { case ((li, ri), e) =>
        Field(
          left.schema(li).name,
          e.ptype,
          left.schema(li).qualifiers ++ right.schema(ri).qualifiers
        )
      }
    val leftRest  = left.schema.fields.indices.filterNot(i => keyIdx.exists(_._1 == i))
    val rightRest = right.schema.fields.indices.filterNot(i => keyIdx.exists(_._2 == i))
    val exprs     =
      keyExprs ++ leftRest.map(i => ColRef(i, left.schema(i).ptype, left.schema(i).name)) ++
        rightRest.map(i => ColRef(lw + i, right.schema(i).ptype, right.schema(i).name))
    val fields = keyFields ++ leftRest.map(left.schema(_)) ++ rightRest.map(right.schema(_))
    ProjectOp(join, exprs.toIndexedSeq, Schema(fields.toIndexedSeq))

  end usingJoin

  /**
    * ASOF join: equalities between the inputs become keys, and the single remaining comparison
    * between the inputs selects the closest right row
    */
  private def planAsOfJoin(j: Join, left: Operator, right: Operator, kind: JoinKind): Operator =
    val condExpr =
      j.cond match
        case JoinOn(e, _) =>
          e
        case JoinOnEq(keys, _) =>
          Expression.concatWithEq(keys)
        case other =>
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(s"Unsupported ASOF join condition: ${other}")
    val conds       = conjuncts(condExpr)
    val (eqs, rest) = conds.partition(c => isEquiBetween(c, left.schema, right.schema))
    def sides(l: Expression, r: Expression): Option[Boolean] =
      if bindsIn(l, left.schema) && !bindsIn(l, right.schema) && bindsIn(r, right.schema) then
        Some(true)
      else if bindsIn(r, left.schema) && !bindsIn(r, right.schema) && bindsIn(l, right.schema) then
        Some(false)
      else
        None
    // (left expression, op as `left op right`, right expression)
    val inequality: (Expression, AsOfOp, Expression) =
      rest match
        case List(c) =>
          val (l, r, op) =
            c match
              case x: GreaterThanOrEq =>
                (x.left, x.right, AsOfOp.Ge)
              case x: GreaterThan =>
                (x.left, x.right, AsOfOp.Gt)
              case x: LessThanOrEq =>
                (x.left, x.right, AsOfOp.Le)
              case x: LessThan =>
                (x.left, x.right, AsOfOp.Lt)
              case other =>
                throw StatusCode
                  .NOT_IMPLEMENTED
                  .newException(s"Unsupported ASOF join inequality: ${other}")
          def flip(o: AsOfOp): AsOfOp =
            o match
              case AsOfOp.Ge =>
                AsOfOp.Le
              case AsOfOp.Gt =>
                AsOfOp.Lt
              case AsOfOp.Le =>
                AsOfOp.Ge
              case AsOfOp.Lt =>
                AsOfOp.Gt
          sides(l, r) match
            case Some(true) =>
              (l, op, r)
            case Some(false) =>
              (r, flip(op), l)
            case None =>
              throw StatusCode
                .NOT_IMPLEMENTED
                .newException(s"ASOF join inequality must compare both inputs: ${c}")
        case _ =>
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException("ASOF joins need exactly one inequality condition")
    val lb       = Binder(this, BindScope(left.schema))
    val rb       = Binder(this, BindScope(right.schema))
    val (lk, rk) = joinKeys(lb, rb, eqs.map(c => equiSides(c, left.schema, right.schema)))
    val (lv, rv) = joinKeys(lb, rb, Seq((inequality._1, inequality._3)))
    val asofKind =
      if kind == JoinKind.Left then
        JoinKind.Left
      else
        JoinKind.Inner
    AsOfJoinOp(
      left,
      right,
      lk,
      rk,
      lv.head,
      rv.head,
      inequality._2,
      asofKind,
      left.schema ++ right.schema
    )

  end planAsOfJoin

end Planner

object Planner:
  /** A planner for a subquery: shares the catalog, sources, and CTE definitions */
  def nested(p: Planner): Planner = p

/** Resolves table names and file paths that are not registered in the catalog */
trait SourceResolver:
  def tableSource(name: String, fullName: String): Option[TableSource]
  def fileSource(path: String): Option[TableSource]
  def parseSql(r: RawSQL): Relation

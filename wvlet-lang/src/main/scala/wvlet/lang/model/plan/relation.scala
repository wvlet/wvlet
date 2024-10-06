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
package wvlet.lang.model.plan

import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.{Name, TermName, TypeName}
import wvlet.lang.model.expr.*
import wvlet.lang.model.*
import wvlet.lang.model.DataType.{
  AggregationType,
  AliasedType,
  ArrayType,
  ConcatType,
  EmptyRelationType,
  NamedType,
  ProjectedType,
  RecordType,
  SchemaType,
  UnresolvedRelationType
}
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.airframe.json.JSON
import wvlet.airframe.ulid.ULID
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

sealed trait Relation extends LogicalPlan:
  // Input attributes (column names) of the relation
  def relationType: RelationType
  override def inputRelationType: RelationType =
    children match
      case Nil =>
        EmptyRelationType
      case (r: Relation) :: Nil =>
        r.relationType
      case lst =>
        val inputs = lst.collect { case r: Relation =>
          r.relationType
        }
        if inputs.isEmpty then
          EmptyRelationType
        else
          RelationTypeList(Name.typeName(ULID.newULIDString), inputs)

  def isPivot: Boolean =
    this match
      case p: Pivot =>
        true
      case _ =>
        false

// A relation that takes a single input relation
trait UnaryRelation extends Relation with UnaryPlan:
  def inputRelation: Relation                  = child
  override def inputRelationType: RelationType = child.relationType
  override def child: Relation

trait HasTableName:
  def name: TableName

case class ModelDef(
    name: TableName,
    params: List[DefArg],
    givenRelationType: Option[RelationType],
    child: Query,
    nodeLocation: Option[NodeLocation]
) extends LogicalPlan
    with HasTableName:
  override def children: Seq[LogicalPlan] = Nil

  override def inputRelationType: RelationType = EmptyRelationType
  override def relationType: RelationType      = givenRelationType.getOrElse(child.relationType)

trait HasRefName extends UnaryRelation:
  def refName: NameExpr

case class SelectAsAlias(child: Relation, alias: NameExpr, nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with HasRefName:
  override def relationType: RelationType = child.relationType
  override def refName: NameExpr          = alias

case class TestRelation(child: Relation, testExpr: Expression, nodeLocation: Option[NodeLocation])
    extends UnaryRelation:
  override def relationType: RelationType = child.relationType

case class ParenthesizedRelation(child: Relation, nodeLocation: Option[NodeLocation])
    extends UnaryRelation:
  override def relationType: RelationType = child.relationType

case class AliasedRelation(
    child: Relation,
    alias: NameExpr,
    columnNames: Option[Seq[NamedType]],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String =
    columnNames match
      case Some(columnNames) =>
        s"AliasedRelation[${alias}](Select[${columnNames.mkString(", ")}](${child}))"
      case None =>
        s"AliasedRelation[${alias}](${child})"

  override lazy val relationType: RelationType =
    columnNames match
      case None =>
        AliasedType(Name.typeName(alias.leafName), child.relationType)
      case Some(columns) =>
        ProjectedType(Name.typeName(alias.leafName), columns, child.relationType)

end AliasedRelation

case class NamedRelation(child: Relation, name: NameExpr, nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with Selection:
  override def toString: String = s"NamedRelation[${name.strExpr}](${child})"

  override def selectItems: Seq[Attribute] =
    // Produce a dummy AllColumns node for SQLGenerator
    Seq(AllColumns(Wildcard(None), None, None))

  override def relationType: RelationType = AliasedType(
    Name.typeName(name.leafName),
    child.relationType
  )

case class Values(rows: Seq[Expression], nodeLocation: Option[NodeLocation])
    extends Relation
    with LeafPlan:
  override def toString: String = s"Values(${rows.mkString(", ")})"

  override val relationType: RelationType =
    // TODO Resolve column types
    UnresolvedRelationType(RelationType.newRelationTypeName)

//  override def outputAttributes: Seq[Attribute] =
//    val values = rows.map { row =>
//      row match
//        case r: RowConstructor =>
//          r.values
//        case other =>
//          Seq(other)
//    }
//    val columns = (0 until values.head.size).map { i =>
//      MultiSourceColumn(EmptyName, values.map(_(i)), None)
//    }
//    columns

/**
  * Reference to a table structured data (tables or other query results)
  * @param name
  * @param nodeLocation
  */
case class TableRef(name: NameExpr, nodeLocation: Option[NodeLocation])
    extends Relation
    with LeafPlan:
  override def toString: String           = s"TableRef(${name})"
  override val relationType: RelationType = UnresolvedRelationType(name.fullName)

case class TableFunctionCall(
    name: NameExpr,
    args: List[FunctionArg],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def toString: String           = s"TableFunctionCall(${name}, ${args})"
  override val relationType: RelationType = UnresolvedRelationType(name.fullName)

case class FileScan(path: String, nodeLocation: Option[NodeLocation])
    extends Relation
    with LeafPlan:
  override def toString: String           = s"FileScan(${path})"
  override val relationType: RelationType = UnresolvedRelationType(RelationType.newRelationTypeName)

case class PathScan(
    name: String,
    path: String,
    schema: RelationType,
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def toString: String           = s"PathScan(${path})"
  override val relationType: RelationType = UnresolvedRelationType(RelationType.newRelationTypeName)

case class JSONFileScan(
    path: String,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def relationType: RelationType =
    if columns.isEmpty then
      schema
    else
      ProjectedType(schema.typeName, columns, schema)

  override def toString: String = s"JSONFileScan(path:${path}, columns:[${columns.mkString(", ")}])"
  override lazy val resolved    = true

case class ParquetFileScan(
    path: String,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def relationType: RelationType =
    if columns.isEmpty then
      schema
    else
      ProjectedType(schema.typeName, columns, schema)

  override def toString: String =
    s"ParquetFileScan(path:${path}, columns:[${columns.mkString(", ")}])"

  override lazy val resolved = true

case class RawSQL(sql: Expression, nodeLocation: Option[NodeLocation])
    extends Relation
    with LeafPlan:
  override val relationType: RelationType = UnresolvedRelationType(RelationType.newRelationTypeName)

case class RawJSON(json: Expression, nodeLocation: Option[NodeLocation])
    extends Relation
    with LeafPlan:
  override val relationType: RelationType = UnresolvedRelationType(RelationType.newRelationTypeName)

/**
  * A relation that only filters rows without changing the schema
  */
trait FilteringRelation extends UnaryRelation:
  override def relationType: RelationType = child.relationType

// Deduplicate (duplicate elimination) the input relation
case class Distinct(child: Project, nodeLocation: Option[NodeLocation])
    extends FilteringRelation
    with AggSelect:
  override def selectItems: Seq[Attribute] = child.selectItems
  override def toString: String            = s"Distinct(${child})"

case class Sort(child: Relation, orderBy: Seq[SortItem], nodeLocation: Option[NodeLocation])
    extends FilteringRelation:
  override def toString: String = s"Sort[${orderBy.mkString(", ")}](${child})"

case class Limit(child: Relation, limit: LongLiteral, nodeLocation: Option[NodeLocation])
    extends FilteringRelation:
  override def toString: String = s"Limit[${limit.value}](${child})"

case class Filter(child: Relation, filterExpr: Expression, nodeLocation: Option[NodeLocation])
    extends FilteringRelation:
  override def toString: String = s"Filter[${filterExpr}](${child})"

case class EmptyRelation(nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  // Need to override this method so as not to create duplicate case object instances
  override def copyInstance(newArgs: Seq[AnyRef]) = this
  override def toString: String                   = s"EmptyRelation()"
  override def relationType: RelationType         = EmptyRelationType

// This node can be a pivot node for generating a SELECT statement
sealed trait Selection extends UnaryRelation:
  def selectItems: Seq[Attribute]

// This node can be a pivot node for generating a SELECT statement with aggregation functions
trait AggSelect extends Selection

case class Project(child: Relation, selectItems: Seq[Attribute], nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with AggSelect:

  override def toString: String = s"Project[${selectItems.mkString(", ")}](${child})"

  override lazy val relationType: RelationType = ProjectedType(
    Name.typeName(RelationType.newRelationTypeName),
    selectItems.map {
      case n: NamedType =>
        n
      case a =>
        NamedType(Name.termName(a.nameExpr.leafName), a.dataType)
    },
    child.relationType
  )

/**
  * Transform a subset of columns in the input relation
  * @param child
  * @param transformItems
  * @param nodeLocation
  */
case class Transform(
    child: Relation,
    transformItems: Seq[Attribute],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with Selection
    with LogSupport:
  override def toString: String = s"Transform[${transformItems.mkString(", ")}](${child})"
  override def selectItems: Seq[Attribute] = transformItems

  override lazy val relationType: RelationType =
    val inputRelation = child.relationType
    val newColumns    = Seq.newBuilder[NamedType]
    // Detect newly added columns
    transformItems.foreach { x =>
      if inputRelation.fields.exists(p => p.name.name != x.fullName) then
        newColumns += NamedType(Name.termName(x.nameExpr.leafName), x.dataType)
    }
    val pt = ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      newColumns.result() ++ child.relationType.fields,
      child.relationType
    )
    pt

case class AddColumnsToRelation(
    child: Relation,
    newColumns: Seq[Attribute],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with LogSupport:
  override def toString: String = s"Add[${newColumns.mkString(", ")}](${child})"

  override lazy val relationType: RelationType =
    val cols = Seq.newBuilder[NamedType]
    cols ++= inputRelationType.fields
    cols ++=
      newColumns.map { x =>
        NamedType(Name.termName(x.nameExpr.leafName), x.dataType)
      }
    val pt = ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      cols.result(),
      child.relationType
    )
    pt

case class ExcludeColumnsFromRelation(
    child: Relation,
    columnNames: Seq[NameExpr],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with LogSupport:
  override def toString: String = s"Drop[${columnNames.mkString(", ")}](${child})"

  override lazy val relationType: RelationType =
    val newColumns = Seq.newBuilder[NamedType]
    // Detect newly added columns
    newColumns ++=
      inputRelationType
        .fields
        .filterNot { x =>
          columnNames.exists(_.leafName == x.name.name)
        }
    val pt = ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      newColumns.result(),
      child.relationType
    )
    pt

case class ShiftColumns(
    child: Relation,
    isLeftShift: Boolean,
    shiftItems: Seq[NameExpr],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String = s"Shift[${shiftItems.mkString(", ")}](${child})"

  override lazy val relationType: RelationType =
    var inputFields: Map[String, NamedType] =
      inputRelationType
        .fields
        .map { f =>
          f.name.name -> f
        }
        .toMap
    val preferredColumns = List.newBuilder[NamedType]
    shiftItems.foreach { si =>
      inputFields.get(si.leafName) match
        case Some(f) =>
          preferredColumns += f
          inputFields -= si.leafName
        case None =>
        // skip
    }
    val remainingColumns = inputFields.values.toList
    ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      // Shift column positions
      if isLeftShift then
        preferredColumns.result() ++ remainingColumns
      else
        remainingColumns ++ preferredColumns.result()
      ,
      child.relationType
    )

end ShiftColumns

/**
  * Aggregation operator that merges records by grouping keys and create a list of records for each
  * group
  * @param child
  * @param groupingKeys
  * @param having
  * @param nodeLocation
  */
case class GroupBy(
    child: Relation,
    groupingKeys: List[GroupingKey],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String = s"GroupBy[${groupingKeys.mkString(",")}](${child})"

  override lazy val relationType: RelationType = AggregationType(
    Name.typeName(RelationType.newRelationTypeName),
    groupingKeys.map {
      case g: UnresolvedGroupingKey =>
        NamedType(Name.termName(g.name.leafName), g.child.dataType)
      case k =>
        NamedType(Name.NoName, k.dataType)
    },
    child.relationType
  )

/**
  * Agg(Select) is an operation to report grouping keys and aggregation expressions.
  *
  * @param child
  * @param selectItems
  * @param nodeLocation
  */
case class Agg(child: Relation, selectItems: List[Attribute], nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with AggSelect:
  override def toString = s"AggSelect[${selectItems.mkString(", ")}](${child})"

  override lazy val relationType: RelationType = ProjectedType(
    Name.typeName(RelationType.newRelationTypeName),
    selectItems.map {
      case n: NamedType =>
        n
      case a =>
        NamedType(Name.termName(a.nameExpr.leafName), a.dataType)
    },
    child.relationType
  )

case class Pivot(
    child: Relation,
    pivotKeys: List[PivotKey],
    groupingKeys: List[GroupingKey],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:

  override def toString =
    s"Pivot(on:[${pivotKeys.mkString(", ")}],group_by:[${groupingKeys.mkString(", ")}],${child})"

  override lazy val relationType: RelationType = AggregationType(
    Name.typeName(RelationType.newRelationTypeName),
    groupingKeys.map {
      case g: UnresolvedGroupingKey =>
        NamedType(Name.termName(g.name.leafName), g.child.dataType)
      case k =>
        NamedType(Name.NoName, k.dataType)
    },
    // TODO Add expanded pivotKey values as fields
    child.relationType
  )

case class PivotKey(name: Identifier, values: List[Literal], nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = values

/**
  * Tradtional SQL aggregation node with SELECT clause
  * @param child
  * @param selectItems
  * @param groupingKeys
  * @param having
  * @param nodeLocation
  */
case class SQLSelect(
    child: Relation,
    selectItems: List[Attribute],
    groupingKeys: List[GroupingKey],
    having: List[Filter],
    filters: List[Filter],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with Selection:
  override def toString =
    s"AggregateSelect[${groupingKeys.mkString(",")}](Select[${selectItems.mkString(", ")}](${child}))"

  override lazy val relationType: RelationType = ProjectedType(
    Name.typeName(RelationType.newRelationTypeName),
    selectItems.map {
      case n: NamedType =>
        n
      case a =>
        NamedType(Name.termName(a.nameExpr.leafName), a.dataType)
    },
    child.relationType
  )

trait QueryStatement extends UnaryRelation

case class Query(body: Relation, nodeLocation: Option[NodeLocation])
    extends QueryStatement
    with FilteringRelation:
  override def child: Relation  = body
  override def toString: String = s"Query(body:${body})"
  override def children: Seq[LogicalPlan] =
    val b = Seq.newBuilder[LogicalPlan]
    b += body
    b.result()

  override def relationType: RelationType = body.relationType

// Joins
case class Join(
    joinType: JoinType,
    left: Relation,
    right: Relation,
    cond: JoinCriteria,
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LogSupport:
  override def modelName: String = joinType.toString

  override def children: Seq[LogicalPlan] = Seq(left, right)

  override def toString: String          = s"${joinType}[${cond}](left:${left}, right:${right})"
  def withCond(cond: JoinCriteria): Join = this.copy(cond = cond)

//
//  override def outputAttributes: Seq[Attribute] =
//    cond match
//      case ju: ResolvedJoinUsing =>
//        val joinKeys = ju.keys
//        val otherAttributes = inputAttributes
//          // Expand AllColumns here
//          .flatMap(_.outputAttributes)
//          .filter { x =>
//            !joinKeys.exists(jk => jk.nameExpr == x.nameExpr)
//          }
//        // report join keys (merged) and other attributes
//        joinKeys ++ otherAttributes
//      case _ =>
//        // Report including duplicated name columns
//        inputAttributes

  override lazy val relationType: RelationType =
    cond match
      case j: JoinOnTheSameColumns =>
        // If the join condition is on the same column names, merge the column types
        val mergedColumns = Seq.newBuilder[NamedType]

        val joinColumns = j.columns.map(_.toTermName)

        mergedColumns ++= left.relationType.fields
        right
          .relationType
          .fields
          .foreach { f =>
            if !joinColumns.contains(f.name) then
              mergedColumns += f
          }
        ProjectedType(
          Name.typeName(RelationType.newRelationTypeName),
          mergedColumns.result(),
          ConcatType(
            Name.typeName(RelationType.newRelationTypeName),
            Seq(left.relationType, right.relationType)
          )
        )
      case _ =>
        ConcatType(
          Name.typeName(RelationType.newRelationTypeName),
          Seq(left.relationType, right.relationType)
        )

end Join

enum JoinType(val symbol: String):
  // Exact match (= equi join)
  case InnerJoin extends JoinType("J")
  // Joins for preserving left table entries
  case LeftOuterJoin extends JoinType("LJ")
  // Joins for preserving right table entries
  case RightOuterJoin extends JoinType("RJ")
  // Joins for preserving both table entries
  case FullOuterJoin extends JoinType("FJ")
  // Cartesian product of two tables
  case CrossJoin extends JoinType("CJ")
  // From clause contains only table names, and
  // Where clause specifies join criteria
  case ImplicitJoin extends JoinType("J")

sealed trait SetOperation extends Relation with LogSupport:
  override def children: Seq[Relation]

  def toSQLOp: String

  override lazy val relationType: RelationType = children
    .headOption
    .map(_.relationType)
    .getOrElse(UnresolvedRelationType(RelationType.newRelationTypeName))

//  override def outputAttributes: Seq[Attribute] = mergeOutputAttributes
//  protected def mergeOutputAttributes: Seq[Attribute] =
//    // Collect all input attributes
//    def collectInputAttributes(rels: Seq[Relation]): Seq[Seq[Attribute]] = rels.flatMap {
//      case s: SetOperation =>
//        collectInputAttributes(s.children)
//      case other =>
//        Seq(
//          other
//            .outputAttributes
//            .flatMap {
//              case a: AllColumns =>
//                a.inputAttributes
//              case other =>
//                other.inputAttributes match
//                  case x if x.length <= 1 =>
//                    x
//                  case inputs =>
//                    Seq(MultiSourceColumn(EmptyName, inputs, None))
//            }
//        )
//    }
//
//    val outputAttributes: Seq[Seq[Attribute]] = collectInputAttributes(children)
//
//    // Verify all relations have the same number of columns
////    require(
////      outputAttributes.map(_.size).distinct.size == 1,
////      "All relations in set operation must have the same number of columns",
////      nodeLocation
////    )
//
//    // Transpose a set of relation columns into a list of same columns
//    // relations: (Ra(a1, a2, ...), Rb(b1, b2, ...))
//    // column lists: ((a1, b1, ...), (a2, b2, ...)
//    val sameColumnList = outputAttributes.transpose
//    sameColumnList.map { columns =>
//      val head = columns.head
//      // val qualifiers = columns.m
//      val distinctColumnNames = columns.map(_.nameExpr).distinctBy(_.leafName)
//      val col = MultiSourceColumn(
//        // If all column has the same name, use it
//        if distinctColumnNames.size == 1 then
//          distinctColumnNames.head
//        else
//          EmptyName
//        ,
//        inputs = columns.toSeq,
//        None
//      )
//        // In set operations, if different column names are merged into one column, the first column name will be used
//        .withAlias(head.nameExpr)
//      col
//    }
//
//  end mergeOutputAttributes

end SetOperation

case class Concat(left: Relation, right: Relation, nodeLocation: Option[NodeLocation])
    extends SetOperation:
  override def toSQLOp: String         = "union all"
  override def children: Seq[Relation] = Seq(left, right)
  override def toString                = s"Concat(${left}, ${right})"

case class Dedup(child: Relation, nodeLocation: Option[NodeLocation]) extends FilteringRelation:
  override def toString = s"Dedup(${child})"

case class Intersect(
    left: Relation,
    right: Relation,
    isDistinct: Boolean,
    nodeLocation: Option[NodeLocation]
) extends SetOperation:
  override def toSQLOp: String =
    s"intersect${
        if isDistinct then
          ""
        else
          " all"
      }"

  override def children: Seq[Relation] = Seq(left, right)
  override def toString                = s"Intersect(${left}, ${right})"

case class Except(
    left: Relation,
    right: Relation,
    isDistinct: Boolean,
    nodeLocation: Option[NodeLocation]
) extends SetOperation:
  override def toSQLOp: String =
    s"except${
        if isDistinct then
          ""
        else
          " all"
      }"

  override def children: Seq[Relation] = Seq(left, right)
  override def toString                = s"Except(${left}, ${right})"

/**
  * Union operation without involving duplicate elimination
  * @param relations
  * @param nodeLocation
  */
case class Union(
    left: Relation,
    right: Relation,
    isDistinct: Boolean,
    nodeLocation: Option[NodeLocation]
) extends SetOperation:
  override def toSQLOp: String =
    s"except${
        if isDistinct then
          ""
        else
          " all"
      }"

  override def children: Seq[Relation] = Seq(left, right)
  override def toString                = s"Union(${children.mkString(",")})"

case class Unnest(
    columns: Seq[Expression],
    withOrdinality: Boolean,
    nodeLocation: Option[NodeLocation]
) extends Relation:
  override def children: Seq[LogicalPlan] = Seq.empty

  override def toString = s"Unnest(withOrdinality:${withOrdinality}, ${columns.mkString(",")})"

  override lazy val relationType: RelationType =
    // TODO
    UnresolvedRelationType(RelationType.newRelationTypeName)

//  override def outputAttributes: Seq[Attribute] = columns.map {
//    case arr: ArrayConstructor =>
//      ResolvedAttribute(Name.termName(ULID.newULIDString), arr.elementType, None, arr.nodeLocation)
//    case other =>
//      SingleColumn(EmptyName, other, other.nodeLocation)
//  }

case class Lateral(query: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def child: Relation = query

  override lazy val relationType: RelationType =
    // TODO
    UnresolvedRelationType(RelationType.newRelationTypeName)

case class LateralView(
    child: Relation,
    exprs: Seq[Expression],
    tableAlias: NameExpr,
    columnAliases: Seq[NameExpr],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:

  override lazy val relationType: RelationType =
    // TODO
    UnresolvedRelationType(RelationType.newRelationTypeName)

/**
  * The lowest level operator to access a table
  *
  * @param fullName
  *   original table reference name in SQL. Used for generating SQL text
  * @param table
  *   source table
  * @param columns
  *   projected columns
  */
case class TableScan(
    name: TableName,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan
    with HasTableName:

  override def relationType: RelationType =
    if columns.isEmpty then
      schema
    else
      ProjectedType(schema.typeName, columns, schema)

  override def toString: String = s"TableScan(name:${name}, columns:[${columns.mkString(", ")}])"

  override lazy val resolved = true

/**
  * Scan a model
  * @param name
  * @param schema
  * @param columns
  * @param nodeLocation
  */
case class ModelScan(
    name: TableName,
    modelArgs: List[FunctionArg],
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan
    with HasTableName:

  override def relationType: RelationType =
    if columns.isEmpty then
      schema
    else
      ProjectedType(schema.typeName, columns, schema)

  override def toString: String = s"ModelScan(name:${name}, schema:${schema})"

  override lazy val resolved = true

case class Subscribe(
    child: Relation,
    name: Identifier,
    watermarkColumn: Option[String],
    windowSize: Option[String],
    params: Seq[SubscribeParam],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override val relationType: RelationType = AliasedType(
    Name.typeName(name.leafName),
    child.relationType
  )

//  def watermarkColumn: Option[String] =
//    params.find(_.name == "watermark_column").map(_.value)
//
//  def windowSize: Option[String] =
//    params.find(_.name == "window_size").map(_.value)

case class SubscribeParam(name: String, value: String, nodeLocation: Option[NodeLocation])
    extends Expression:
  override def children: Seq[Expression] = Seq.empty

case class IncrementalTableScan(
    name: TableName,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan
    with HasTableName:

  override val relationType: RelationType = ProjectedType(schema.typeName, columns, schema)

  override def toString: String =
    s"IncrementalScan(name:${name}, columns:[${columns.mkString(", ")}])"

case class IncrementalAppend(
    // Differential query
    child: Relation,
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def relationType: RelationType = child.relationType

enum ShowType:
  case models
  case tables

case class Show(showType: ShowType, nodeLocation: Option[NodeLocation])
    extends Relation
    with LeafPlan:
  override def relationType: RelationType =
    showType match
      case ShowType.models =>
        SchemaType(
          parent = None,
          typeName = Name.typeName("model"),
          columnTypes = Seq[NamedType](
            NamedType(Name.termName("name"), DataType.StringType),
            NamedType(Name.termName("args"), DataType.StringType),
            NamedType(Name.termName("package_name"), DataType.StringType)
          )
        )
      case ShowType.tables =>
        SchemaType(
          parent = None,
          typeName = Name.typeName("table"),
          columnTypes = Seq[NamedType](NamedType(Name.termName("name"), DataType.StringType))
        )

trait RelationInspector extends QueryStatement

case class Describe(child: Relation, nodeLocation: Option[NodeLocation]) extends RelationInspector:
  override def relationType: RelationType = SchemaType(
    parent = None,
    typeName = Name.typeName("table_description"),
    columnTypes = List(
      NamedType(Name.termName("column_name"), DataType.StringType),
      NamedType(Name.termName("column_type"), DataType.StringType)
    )
  )

  def descRows: Seq[ListMap[String, Any]] = child
    .relationType
    .fields
    .map { f =>
      ListMap("column_name" -> f.name.name, "column_type" -> f.dataType.typeDescription)
    }

case class Sample(
    child: Relation,
    method: SamplingMethod,
    size: SamplingSize,
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def relationType: RelationType = child.relationType

enum SamplingMethod:
  case reservoir
  case system
  case bernoulli

enum SamplingSize:
  case Rows(rows: Long)               extends SamplingSize
  case Percentage(percentage: Double) extends SamplingSize

/**
  * Debug operator adds a separate execution path to inspect the input relation.
  * @param child
  *   query fragment to debug
  * @param debugRelation
  *   a chain of operators for debugging
  * @param nodeLocation
  */
case class Debug(child: Relation, debugExpr: Relation, nodeLocation: Option[NodeLocation])
    extends FilteringRelation:

  // Add debug expr as well for the ease of tree traversal
  override def children: Seq[LogicalPlan] = Seq(child, debugExpr)

package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.compiler.{Name, TypeName}
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.*
import com.treasuredata.flow.lang.model.DataType.{
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
import com.treasuredata.flow.lang.model.expr.NameExpr.EmptyName
import wvlet.airframe.json.JSON
import wvlet.airframe.ulid.ULID
import wvlet.log.LogSupport

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

// A relation that takes a single input relation
trait UnaryRelation extends Relation with UnaryPlan:
  def inputRelation: Relation                  = child
  override def inputRelationType: RelationType = child.inputRelationType
  override def child: Relation

trait HasName:
  def name: Name

case class ModelDef(
    name: Name,
    params: List[DefArg],
    givenRelationType: Option[RelationType],
    child: Relation,
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with HasName:
  override def relationType: RelationType = givenRelationType.getOrElse(child.relationType)

case class TestRelation(
    child: Relation,
    textExprs: Seq[Expression],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def relationType: RelationType = child.relationType

case class ParenthesizedRelation(child: Relation, nodeLocation: Option[NodeLocation])
    extends UnaryRelation:
  override def relationType: RelationType = child.relationType

case class AliasedRelation(
    child: Relation,
    alias: NameExpr,
    columnNames: Option[Seq[NameExpr]],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String =
    columnNames match
      case Some(columnNames) =>
        s"AliasedRelation[${alias}](Select[${columnNames.mkString(", ")}](${child}))"
      case None =>
        s"AliasedRelation[${alias}](${child})"

  override def relationType: RelationType =
    // TODO column rename with columnNames
    AliasedType(Name.typeName(alias.leafName), child.relationType)

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

case class ModelRef(name: NameExpr, args: List[FunctionArg], nodeLocation: Option[NodeLocation])
    extends Relation
    with LeafPlan:
  override def toString: String           = s"ModelRef(${name}, ${args})"
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

case class RawSQL(sql: String, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override val relationType: RelationType = UnresolvedRelationType(RelationType.newRelationTypeName)

// Deduplicate (duplicate elimination) the input relation
case class Distinct(child: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String           = s"Distinct(${child})"
  override def relationType: RelationType = child.relationType

case class Sort(child: Relation, orderBy: Seq[SortItem], nodeLocation: Option[NodeLocation])
    extends UnaryRelation:
  override def toString: String           = s"Sort[${orderBy.mkString(", ")}](${child})"
  override def relationType: RelationType = child.relationType

case class Limit(child: Relation, limit: LongLiteral, nodeLocation: Option[NodeLocation])
    extends UnaryRelation:
  override def toString: String           = s"Limit[${limit.value}](${child})"
  override def relationType: RelationType = child.relationType

case class Filter(child: Relation, filterExpr: Expression, nodeLocation: Option[NodeLocation])
    extends UnaryRelation:
  override def toString: String           = s"Filter[${filterExpr}](${child})"
  override def relationType: RelationType = child.relationType

case class EmptyRelation(nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  // Need to override this method so as not to create duplicate case object instances
  override def copyInstance(newArgs: Seq[AnyRef]) = this
  override def toString: String                   = s"EmptyRelation()"
  override def relationType: RelationType         = EmptyRelationType

// This node can be a pivot node for generating a SELECT statement
sealed trait Selection extends UnaryRelation:
  def selectItems: Seq[Attribute]

case class Project(child: Relation, selectItems: Seq[Attribute], nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with Selection:

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

/**
  * Aggregation operator that merges records by grouping keys and create a list of records for each
  * group
  * @param child
  * @param groupingKeys
  * @param having
  * @param nodeLocation
  */
case class Aggregate(
    child: Relation,
    groupingKeys: List[GroupingKey],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String = s"Aggregate[${groupingKeys.mkString(",")}](${child})"

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
  * Tradtional aggregation node with SELECT clause
  * @param child
  * @param selectItems
  * @param groupingKeys
  * @param having
  * @param nodeLocation
  */
case class AggregateSelect(
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

case class Query(body: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
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

  override lazy val relationType: RelationType = ConcatType(
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

case class Intersect(relations: Seq[Relation], nodeLocation: Option[NodeLocation])
    extends SetOperation:
  override def children: Seq[Relation] = relations
  override def toString                = s"Intersect(${relations.mkString(", ")})"

case class Except(left: Relation, right: Relation, nodeLocation: Option[NodeLocation])
    extends SetOperation:
  override def children: Seq[Relation] = Seq(left, right)

  override def toString = s"Except(${left}, ${right})"

/**
  * Union operation without involving duplicate elimination
  * @param relations
  * @param nodeLocation
  */
case class Union(relations: Seq[Relation], nodeLocation: Option[NodeLocation]) extends SetOperation:
  override def children: Seq[Relation] = relations
  override def toString                = s"Union(${relations.mkString(",")})"

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
    name: Name,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan
    with HasName:

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
    name: Name,
    modelArgs: List[FunctionArg],
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan
    with HasName:

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
    name: Name,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan
    with HasName:

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

package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.catalog.Catalog
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
import wvlet.airframe.json.JSON
import wvlet.airframe.ulid.ULID
import wvlet.log.LogSupport

sealed trait Relation extends LogicalPlan:
  def relationType: RelationType
  def inputRelationTypes: Seq[RelationType] = children
    .collect { case r: Relation => r }
    .map(_.relationType)

// A relation that takes a single input relation
trait UnaryRelation extends Relation with UnaryPlan:
  def inputRelation: Relation = child
  override def child: Relation

case class ParenthesizedRelation(child: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def outputAttributes: Seq[Attribute] = child.outputAttributes
  override def relationType: RelationType       = child.relationType

case class AliasedRelation(
    child: Relation,
    alias: Identifier,
    columnNames: Option[Seq[String]],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String =
    columnNames match
      case Some(columnNames) =>
        s"AliasedRelation[${alias}](Select[${columnNames.mkString(", ")}](${child}))"
      case None =>
        s"AliasedRelation[${alias}](${child})"

  override def relationType: RelationType =
    columnNames match
      case Some(cols) =>
        ProjectedType(
          alias.value,
          cols.map { col =>
            val colType = child.outputAttributes.find(_.name == col).map(_.dataType).getOrElse(DataType.UnknownType)
            NamedType(col, colType)
          },
          child.relationType
        )
      case None =>
        AliasedType(
          alias.value,
          child.relationType
        )

  override def outputAttributes: Seq[Attribute] =
    val qualifier = Qualifier.parse(alias.value)
    val attrs     = child.outputAttributes.map(_.withQualifier(qualifier))
    val result = columnNames match
      case Some(columnNames) =>
        attrs.zip(columnNames).map { case (a, columnName) =>
          a.withAlias(columnName)
        }
      case None =>
        attrs
    result

case class NamedRelation(
    child: Relation,
    name: Identifier,
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with Selection:
  override def toString: String = s"NamedRelation[${name.value}](${child})"
  override def outputAttributes: Seq[Attribute] =
    val qual = Qualifier.parse(name.value)
    child.outputAttributes.map(_.withQualifier(qual))

  override def selectItems: Seq[Attribute] =
    // Produce a dummy AllColumns node for SQLGenerator
    Seq(AllColumns(Qualifier.empty, None, None))

  override def relationType: RelationType = AliasedType(name.value, child.relationType)

case class Values(rows: Seq[Expression], nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def toString: String = s"Values(${rows.mkString(", ")})"

  override def relationType: RelationType =
    // TODO Resolve column types
    UnresolvedRelationType(RelationType.newRelationTypeName)

  override def outputAttributes: Seq[Attribute] =
    val values = rows.map { row =>
      row match
        case r: RowConstructor => r.values
        case other             => Seq(other)
    }
    val columns = (0 until values.head.size).map { i =>
      MultiSourceColumn(values.map(_(i)), Qualifier.empty, None)
    }
    columns

/**
  * Reference to a table structured data (tables or other query results)
  * @param name
  * @param nodeLocation
  */
case class TableRef(name: QName, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def toString: String                 = s"TableRef(${name})"
  override def outputAttributes: Seq[Attribute] = Nil
  override def relationType: RelationType       = UnresolvedRelationType(name.fullName)

case class FileScan(path: String, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def toString: String                 = s"FileScan(${path})"
  override def outputAttributes: Seq[Attribute] = Nil
  override def relationType: RelationType       = UnresolvedRelationType(RelationType.newRelationTypeName)

case class JSONFileScan(
    path: String,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def inputAttributes: Seq[Attribute] = Seq.empty

  override def outputAttributes: Seq[Attribute] =
    columns.map { col =>
      ResolvedAttribute(
        col.name,
        col,
        Qualifier.empty, // This must be empty first
        None,            // TODO Some(SourceColumn(table, col)),
        None             // ResolvedAttribute always has no NodeLocation
      )
    }

  override def relationType: RelationType =
    ProjectedType(schema.typeName, columns, schema)

  override def toString: String =
    s"ResolvedFileScan(path:${path}, columns:[${columns.mkString(", ")}])"

  override lazy val resolved = true

case class RawSQL(sql: String, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def outputAttributes: Seq[Attribute] = Nil
  override def relationType: RelationType       = UnresolvedRelationType(RelationType.newRelationTypeName)

// Deduplicate (duplicate elimination) the input relation
case class Distinct(child: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Distinct(${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes
  override def relationType: RelationType       = child.relationType

case class Sort(child: Relation, orderBy: Seq[SortItem], nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Sort[${orderBy.mkString(", ")}](${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes
  override def relationType: RelationType       = child.relationType

case class Limit(child: Relation, limit: LongLiteral, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Limit[${limit.value}](${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes
  override def relationType: RelationType       = child.relationType

case class Filter(child: Relation, filterExpr: Expression, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Filter[${filterExpr}](${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes
  override def relationType: RelationType       = child.relationType

case class EmptyRelation(nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  // Need to override this method so as not to create duplicate case object instances
  override def copyInstance(newArgs: Seq[AnyRef]) = this
  override def toString: String                   = s"EmptyRelation()"
  override def outputAttributes: Seq[Attribute]   = Nil
  override def relationType: RelationType         = EmptyRelationType

// This node can be a pivot node for generating a SELECT statement
sealed trait Selection extends UnaryRelation:
  def selectItems: Seq[Attribute]

case class Project(child: Relation, selectItems: Seq[Attribute], nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with Selection:

  override def toString: String                 = s"Project[${selectItems.mkString(", ")}](${child})"
  override def outputAttributes: Seq[Attribute] = selectItems

  override lazy val relationType: RelationType =
    ProjectedType(
      RelationType.newRelationTypeName,
      selectItems.map {
        case n: NamedType => n
        case a =>
          NamedType(a.name, a.dataType)
      },
      child.relationType
    )

/**
  * Transform a subset of columns in the input relation
  * @param child
  * @param transformItems
  * @param nodeLocation
  */
case class Transform(child: Relation, transformItems: Seq[Attribute], nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with Selection:
  override def toString: String                 = s"Transform[${transformItems.mkString(", ")}](${child})"
  override def outputAttributes: Seq[Attribute] = transformItems ++ child.outputAttributes

  override def selectItems: Seq[Attribute] = outputAttributes

  override def relationType: RelationType =
    // TODO detect newly added columns and overriden columns
    child.relationType

/**
  * Aggregation operator that merges records by grouping keys and create a list of records for each group
  * @param child
  * @param groupingKeys
  * @param having
  * @param nodeLocation
  */
case class Aggregate(
    child: Relation,
    groupingKeys: List[GroupingKey],
    having: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String = s"Aggregate[${groupingKeys.mkString(",")}](${child})"
  override def outputAttributes: Seq[Attribute] =
    val keyAttrs = groupingKeys.map(key => SingleColumn(key, Qualifier.empty, key.nodeLocation))
    // TODO change type as ((k1, k2) -> Seq[c1, c2, c3, ...]) type
    keyAttrs ++ child.outputAttributes

  override lazy val relationType: RelationType =
    AggregationType(
      RelationType.newRelationTypeName,
      groupingKeys.map(k => k.dataType),
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
    having: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with Selection:
  override def toString =
    s"AggregateSelect[${groupingKeys.mkString(",")}](Select[${selectItems.mkString(", ")}](${child}))"

  override def outputAttributes: Seq[Attribute] = selectItems
  override lazy val relationType: RelationType =
    ProjectedType(
      RelationType.newRelationTypeName,
      selectItems.map {
        case n: NamedType => n
        case a =>
          NamedType(a.name, a.dataType)
      },
      child.relationType
    )

case class Query(body: Relation, nodeLocation: Option[NodeLocation]) extends Relation:
  override def toString: String = s"Query(body:${body})"
  override def children: Seq[LogicalPlan] =
    val b = Seq.newBuilder[LogicalPlan]
    b += body
    b.result()

  override def outputAttributes: Seq[Attribute] = body.outputAttributes
  override def relationType: RelationType       = body.relationType

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

  override def inputAttributes: Seq[Attribute] =
    left.outputAttributes ++ right.outputAttributes

  override def outputAttributes: Seq[Attribute] =
    cond match
      case ju: ResolvedJoinUsing =>
        val joinKeys = ju.keys
        val otherAttributes = inputAttributes
          // Expand AllColumns here
          .flatMap(_.outputAttributes)
          .filter { x =>
            !joinKeys.exists(jk => jk.name == x.name)
          }
        // report join keys (merged) and other attributes
        joinKeys ++ otherAttributes
      case _ =>
        // Report including duplicated name columns
        inputAttributes

  override lazy val relationType: RelationType =
    ConcatType(RelationType.newRelationTypeName, Seq(left.relationType, right.relationType))

sealed abstract class JoinType(val symbol: String)

// Exact match (= equi join)
case object InnerJoin extends JoinType("J")

// Joins for preserving left table entries
case object LeftOuterJoin extends JoinType("LJ")

// Joins for preserving right table entries
case object RightOuterJoin extends JoinType("RJ")

// Joins for preserving both table entries
case object FullOuterJoin extends JoinType("FJ")

// Cartesian product of two tables
case object CrossJoin extends JoinType("CJ")

// From clause contains only table names, and
// Where clause specifies join criteria
case object ImplicitJoin extends JoinType("J")

sealed trait SetOperation extends Relation with LogSupport:
  override def children: Seq[Relation]

  override def outputAttributes: Seq[Attribute] = mergeOutputAttributes

  override lazy val relationType: RelationType =
    children.headOption.map(_.relationType).getOrElse(UnresolvedRelationType(RelationType.newRelationTypeName))

  protected def mergeOutputAttributes: Seq[Attribute] =
    // Collect all input attributes
    def collectInputAttributes(rels: Seq[Relation]): Seq[Seq[Attribute]] =
      rels.flatMap {
        case s: SetOperation => collectInputAttributes(s.children)
        case other =>
          Seq(other.outputAttributes.flatMap {
            case a: AllColumns => a.inputAttributes
            case other =>
              other.inputAttributes match
                case x if x.length <= 1 => x
                case inputs             => Seq(MultiSourceColumn(inputs, Qualifier.empty, None))
          })
      }

    val outputAttributes: Seq[Seq[Attribute]] = collectInputAttributes(children)

    // Verify all relations have the same number of columns
//    require(
//      outputAttributes.map(_.size).distinct.size == 1,
//      "All relations in set operation must have the same number of columns",
//      nodeLocation
//    )

    // Transpose a set of relation columns into a list of same columns
    // relations: (Ra(a1, a2, ...), Rb(b1, b2, ...))
    // column lists: ((a1, b1, ...), (a2, b2, ...)
    val sameColumnList = outputAttributes.transpose
    sameColumnList.map { columns =>
      val head       = columns.head
      val qualifiers = columns.map(_.qualifier).distinct
      val col = MultiSourceColumn(
        inputs = columns.toSeq,
        qualifier =
          // If all of the qualifiers are the same, use it.
          if qualifiers.size == 1 then qualifiers.head
          else Qualifier.empty,
        None
      )
        // In set operations, if different column names are merged into one column, the first column name will be used
        .withAlias(head.name)
      col
    }.toSeq

case class Intersect(
    relations: Seq[Relation],
    nodeLocation: Option[NodeLocation]
) extends SetOperation:
  override def children: Seq[Relation] = relations

  override def toString =
    s"Intersect(${relations.mkString(", ")})"

case class Except(left: Relation, right: Relation, nodeLocation: Option[NodeLocation]) extends SetOperation:
  override def children: Seq[Relation] = Seq(left, right)

  override def toString =
    s"Except(${left}, ${right})"

  override def inputAttributes: Seq[Attribute] = left.inputAttributes

  override def outputAttributes: Seq[Attribute] = left.outputAttributes

/**
  * Union operation without involving duplicate elimination
  * @param relations
  * @param nodeLocation
  */
case class Union(
    relations: Seq[Relation],
    nodeLocation: Option[NodeLocation]
) extends SetOperation:
  override def children: Seq[Relation] = relations

  override def toString =
    s"Union(${relations.mkString(",")})"

case class Unnest(columns: Seq[Expression], withOrdinality: Boolean, nodeLocation: Option[NodeLocation])
    extends Relation:
  override def children: Seq[LogicalPlan] = Seq.empty

  override def toString =
    s"Unnest(withOrdinality:${withOrdinality}, ${columns.mkString(",")})"

  override lazy val relationType: RelationType =
    // TODO
    UnresolvedRelationType(RelationType.newRelationTypeName)

  override def inputAttributes: Seq[Attribute] = Seq.empty // TODO
  override def outputAttributes: Seq[Attribute] =
    columns.map {
      case arr: ArrayConstructor =>
        ResolvedAttribute(ULID.newULIDString, arr.elementType, Qualifier.empty, None, arr.nodeLocation)
      case other =>
        SingleColumn(other, Qualifier.empty, other.nodeLocation)
    }

case class Lateral(query: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def child: Relation = query

  override lazy val relationType: RelationType =
    // TODO
    UnresolvedRelationType(RelationType.newRelationTypeName)

  override def outputAttributes: Seq[Attribute] =
    query.outputAttributes // TODO

case class LateralView(
    child: Relation,
    exprs: Seq[Expression],
    tableAlias: Identifier,
    columnAliases: Seq[Identifier],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:

  override lazy val relationType: RelationType =
    // TODO
    UnresolvedRelationType(RelationType.newRelationTypeName)

  override def outputAttributes: Seq[Attribute] =
    columnAliases.map(x => UnresolvedAttribute(Qualifier.parse(tableAlias.value), x.value, None))

/**
  * The lowest level operator to access a table
  *
  * @param fullName
  *   original table reference name in SQL. Used for generating SQL text
  * @param table
  *   source table
  * @param columns
  *   projected colfumns
  */
case class TableScan(
    name: String,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def inputAttributes: Seq[Attribute] = Seq.empty
  override def outputAttributes: Seq[Attribute] =
    columns.map { col =>
      ResolvedAttribute(
        col.name,
        col,
        Qualifier.empty, // This must be empty first
        None,            // TODO Some(SourceColumn(table, col)),
        None             // ResolvedAttribute always has no NodeLocation
      )
    }

  override def relationType: RelationType =
    ProjectedType(schema.typeName, columns, schema)

  override def toString: String =
    s"TableScan(name:${name}, columns:[${columns.mkString(", ")}])"

  override lazy val resolved = true

/**
  * Scan a table-structured data reference, such as a query result or table
  * @param name
  * @param schema
  * @param columns
  * @param nodeLocation
  */
case class RelScan(
    name: String,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def inputAttributes: Seq[Attribute] = Seq.empty
  override def outputAttributes: Seq[Attribute] = columns.map { col =>
    ResolvedAttribute(
      col.name,
      col,
      Qualifier.empty, // This must be empty first
      None,            // TODO Some(SourceColumn(table, col)),
      None             // ResolvedAttribute always has no NodeLocation
    )
  }

  override def relationType: RelationType = schema
  override def toString: String =
    s"RelScan(name:${name}, schema:${schema})"

  override lazy val resolved = true

case class Subscribe(
    child: Relation,
    name: Identifier,
    watermarkColumn: Option[String],
    windowSize: Option[String],
    params: Seq[SubscribeParam],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override val relationType: RelationType = AliasedType(name.value, child.relationType)

//  def watermarkColumn: Option[String] =
//    params.find(_.name == "watermark_column").map(_.value)
//
//  def windowSize: Option[String] =
//    params.find(_.name == "window_size").map(_.value)

  override def inputAttributeList: AttributeList = child.inputAttributeList
  override def outputAttributes: Seq[Attribute]  = child.outputAttributes

case class SubscribeParam(name: String, value: String, nodeLocation: Option[NodeLocation]) extends Expression:
  override def children: Seq[Expression] = Seq.empty

case class IncrementalTableScan(
    name: String,
    schema: RelationType,
    columns: Seq[NamedType],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def inputAttributes: Seq[Attribute] = Seq.empty

  override def outputAttributes: Seq[Attribute] =
    columns.map { col =>
      ResolvedAttribute(
        col.name,
        col,
        Qualifier.empty,
        None,
        None
      )
    }

  override def relationType: RelationType =
    ProjectedType(schema.typeName, columns, schema)

  override def toString: String =
    s"IncrementalScan(name:${name}, columns:[${columns.mkString(", ")}])"

case class IncrementalAppend(
    // Differential query
    child: Relation,
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def relationType: RelationType       = child.relationType
  override def outputAttributes: Seq[Attribute] = child.outputAttributes

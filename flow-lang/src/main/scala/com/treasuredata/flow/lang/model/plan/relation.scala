package com.treasuredata.flow.lang.model.plan

import com.treasuredata.flow.lang.catalog.Catalog
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.*
import wvlet.airframe.ulid.ULID
import wvlet.log.LogSupport

sealed trait Relation extends LogicalPlan

// A relation that takes a single input relation
trait UnaryRelation extends Relation with UnaryPlan:
  def inputRelation: Relation = child
  override def child: Relation

case class ParenthesizedRelation(child: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def outputAttributes: Seq[Attribute] = child.outputAttributes

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

  override def outputAttributes: Seq[Attribute] =
    val qualifier = Qualifier.parse(alias.value)
    val attrs     = child.outputAttributes.map(_.withQualifier(qualifier))
    val result = columnNames match
      case Some(columnNames) =>
        attrs.zip(columnNames).map { case (a, columnName) =>
          a match
            case a: Attribute => a.withAlias(columnName)
            case others       => others
        }
      case None =>
        attrs
    result

case class NamedRelation(
    child: Relation,
    name: Identifier,
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
  override def toString: String = s"NamedRelation[${name.value}](${child})"
  override def outputAttributes: Seq[Attribute] =
    val qual = Qualifier.parse(name.value)
    child.outputAttributes.map(_.withQualifier(qual))

case class Values(rows: Seq[Expression], nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def toString: String = s"Values(${rows.mkString(", ")})"

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

case class TableRef(name: QName, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def toString: String                 = s"TableRef(${name})"
  override def outputAttributes: Seq[Attribute] = Nil

case class RawSQL(sql: String, nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  override def outputAttributes: Seq[Attribute] = Nil

// Deduplicate (duplicate elimination) the input relation
case class Distinct(child: Relation, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Distinct(${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes

case class Sort(child: Relation, orderBy: Seq[SortItem], nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Sort[${orderBy.mkString(", ")}](${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes

case class Limit(child: Relation, limit: LongLiteral, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Limit[${limit.value}](${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes

case class Filter(child: Relation, filterExpr: Expression, nodeLocation: Option[NodeLocation]) extends UnaryRelation:
  override def toString: String                 = s"Filter[${filterExpr}](${child})"
  override def outputAttributes: Seq[Attribute] = child.outputAttributes

case class EmptyRelation(nodeLocation: Option[NodeLocation]) extends Relation with LeafPlan:
  // Need to override this method so as not to create duplicate case object instances
  override def copyInstance(newArgs: Seq[AnyRef]) = this
  override def toString: String                   = s"EmptyRelation()"
  override def outputAttributes: Seq[Attribute]   = Nil

// This node can be a pivot node for generating a SELECT statement
sealed trait Selection extends UnaryRelation:
  def selectItems: Seq[Attribute]

case class Project(child: Relation, selectItems: Seq[Attribute], nodeLocation: Option[NodeLocation])
    extends UnaryRelation
    with Selection:

  override def toString: String                 = s"Project[${selectItems.mkString(", ")}](${child})"
  override def outputAttributes: Seq[Attribute] = selectItems

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

case class AggregateSelect(
    child: Relation,
    selectItems: List[Attribute],
    groupingKeys: List[GroupingKey],
    having: Option[Expression],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation
    with Selection:
  override def toString =
    s"Aggregate[${groupingKeys.mkString(",")}](Select[${selectItems.mkString(", ")}](${child}))"

  override def outputAttributes: Seq[Attribute] = selectItems

case class Query(body: Relation, nodeLocation: Option[NodeLocation]) extends Relation:
  override def toString: String = s"Query(body:${body})"
  override def children: Seq[LogicalPlan] =
    val b = Seq.newBuilder[LogicalPlan]
    b += body
    b.result()

  override def outputAttributes: Seq[Attribute] = body.outputAttributes

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

  override def outputAttributes: Seq[Attribute] =
    query.outputAttributes // TODO

case class LateralView(
    child: Relation,
    exprs: Seq[Expression],
    tableAlias: Identifier,
    columnAliases: Seq[Identifier],
    nodeLocation: Option[NodeLocation]
) extends UnaryRelation:
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
  *   projectec columns
  */
case class TableScan(
    fullName: String,
    table: Catalog.Table,
    columns: Seq[Catalog.TableColumn],
    nodeLocation: Option[NodeLocation]
) extends Relation
    with LeafPlan:
  override def inputAttributes: Seq[Attribute] = Seq.empty
  override def outputAttributes: Seq[Attribute] =
    columns.map { col =>
      ResolvedAttribute(
        col.name,
        col.dataType,
        Qualifier.empty, // This must be empty first
        Some(SourceColumn(table, col)),
        None // ResolvedAttribute always has no NodeLocation
      )
    }

  override def toString: String =
    s"TableScan(name:${fullName}, table:${table.fullName}, columns:[${columns.mkString(", ")}])"

  override lazy val resolved = true

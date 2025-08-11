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

import wvlet.airframe.ulid.ULID
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.{Span, StatusCode}
import wvlet.lang.catalog.Catalog
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.{Name, TermName}
import wvlet.lang.model.*
import wvlet.lang.model.DataType.*
import wvlet.lang.model.expr.*
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

trait Relation extends LogicalPlan:
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

  def isLeaf: Boolean = children.isEmpty

// A relation that takes a single input relation
trait UnaryRelation extends Relation with UnaryPlan:
  def inputRelation: Relation                  = child
  override def inputRelationType: RelationType = child.relationType
  override def child: Relation

trait HasTableName:
  def name: TableName

type TableOrFileName = StringLiteral | QualifiedName

/**
  * A common trait for using a table name or a file name
  */
trait HasTableOrFileName:
  def target: TableOrFileName
  def isForFile: Boolean =
    target match
      case l: StringLiteral =>
        true
      case q: QualifiedName =>
        false

  def isForTable: Boolean = !isForFile
  def targetName: String =
    target match
      case l: StringLiteral =>
        l.unquotedValue
      case q: QualifiedName =>
        q.fullName

case class SelectAsAlias(child: Relation, target: QualifiedName, span: Span)
    extends UnaryRelation
    with HasTableOrFileName:
  override def relationType: RelationType = child.relationType

case class TestRelation(child: Relation, testExpr: Expression, span: Span) extends UnaryRelation:
  override def relationType: RelationType = child.relationType

case class BracedRelation(child: Relation, span: Span) extends UnaryRelation:
  override def relationType: RelationType = child.relationType

case class AliasedRelation(
    child: Relation,
    alias: NameExpr,
    columnNames: Option[List[NamedType]],
    span: Span
) extends UnaryRelation
    with LogSupport:
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
        // Assign the given field names to the input relation fields
        val typedColumns = columns
          .zip(child.relationType.fields)
          .map { (c, f) =>
            val dt =
              if c.isResolved then
                c
              else
                f.dataType
            NamedType(c.name, dt)
          }
        ProjectedType(Name.typeName(alias.leafName), typedColumns, child.relationType)

end AliasedRelation

case class NamedRelation(child: Relation, name: NameExpr, span: Span)
    extends UnaryRelation
    with Selection:
  override def toString: String = s"NamedRelation[${name.fullName}](${child})"

  override def selectItems: List[Attribute] =
    // Produce a dummy AllColumns node for SQLGenerator
    List(AllColumns(Wildcard(NoSpan), None, NoSpan))

  override def relationType: RelationType = AliasedType(
    Name.typeName(name.leafName),
    child.relationType
  )

case class Values(rows: List[Expression], span: Span) extends Relation with LeafPlan:
  override def toString: String = s"Values(${rows.mkString(", ")})"

  override val relationType: RelationType =
    if rows.isEmpty then
      EmptyRelationType
    else
      val row = rows.head
      row match
        case arr: ArrayConstructor =>
          val columns = arr
            .values
            .map { v =>
              NamedType(Name.NoName, v.dataType)
            }
          SchemaType(None, Name.NoTypeName, columnTypes = columns)
        case _ =>
          EmptyRelationType

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

trait TableInput extends Relation with LeafPlan:
  /**
    * SQL expression that represents this table input.
    */
  def sqlExpr: Expression
  // def alias: NameExpr
  // def columnNames: Option[Seq[NamedType]]

/**
  * Reference to a table structured data (tables or other query results)
  * @param name
  * @param nodeLocation
  */
case class TableRef(name: QualifiedName, span: Span) extends TableInput:
  override def sqlExpr: Expression = name
  // TODO Fix to generate a correct Wvlet expression for double-quoted or dot-separated table names
  override def toString: String           = s"TableRef(${name})"
  override val relationType: RelationType = UnresolvedRelationType(name.fullName)

case class TableFunctionCall(name: NameExpr, args: List[FunctionArg], span: Span)
    extends TableInput:
  override def sqlExpr: Expression        = FunctionApply(name, args, None, span)
  override def toString: String           = s"TableFunctionCall(${name}, ${args})"
  override val relationType: RelationType = UnresolvedRelationType(name.fullName)

case class FileRef(path: StringLiteral, span: Span) extends TableInput:
  def filePath: String                    = path.unquotedValue
  override def sqlExpr: Expression        = path
  override def toString: String           = s"FileRef(${path})"
  override val relationType: RelationType = UnresolvedRelationType(path.unquotedValue)

case class FileScan(
    path: StringLiteral,
    // The schema of the file or UnknownRelationType before checking the file
    schema: RelationType,
    // Projected columns
    columns: List[NamedType],
    span: Span
) extends TableInput:
  def filePath: String             = path.unquotedValue
  override def sqlExpr: Expression = path
  override def toString: String    = s"FileScan(${filePath})"
  override val relationType: RelationType =
    if columns.isEmpty then
      schema
    else
      ProjectedType(schema.typeName, columns, schema)

case class RawSQL(sql: Expression, span: Span) extends TableInput:
  override def sqlExpr: Expression        = sql
  override val relationType: RelationType = UnresolvedRelationType(RelationType.newRelationTypeName)

case class RawJSON(json: Expression, span: Span) extends TableInput:
  override def sqlExpr: Expression        = json
  override val relationType: RelationType = UnresolvedRelationType(RelationType.newRelationTypeName)

// A base trait that will be translated to SELECT * in SQL
trait GeneralSelection extends UnaryRelation

/**
  * A relation that only filters rows without changing the schema
  */
trait FilteringRelation extends GeneralSelection:
  override def relationType: RelationType = child.relationType

// Deduplicate (duplicate elimination) the input relation
case class Distinct(child: Project, span: Span) extends FilteringRelation with AggSelect:
  override def selectItems: List[Attribute] = child.selectItems
  override def toString: String             = s"Distinct(${child})"

case class Sort(child: Relation, orderBy: List[SortItem], span: Span) extends FilteringRelation:
  override def toString: String = s"Sort[${orderBy.mkString(", ")}](${child})"

case class Limit(child: Relation, limit: LongLiteral, span: Span) extends FilteringRelation:
  override def toString: String = s"Limit[${limit.value}](${child})"

case class Offset(child: Relation, rows: LongLiteral, span: Span) extends FilteringRelation:
  override def toString: String = s"Offset[${rows.value}](${child})"

case class Filter(child: Relation, filterExpr: Expression, span: Span) extends FilteringRelation:
  override def toString: String = s"Filter[${filterExpr}](${child})"

case class Count(child: Relation, span: Span) extends UnaryRelation with AggSelect:
  override def toString: String = s"Count(${child})"

  override def selectItems: List[Attribute] = List(
    SingleColumn(
      NameExpr.EmptyName,
      FunctionApply(
        NameExpr.fromString("count", span),
        List(FunctionArg(None, AllColumns(Wildcard(NoSpan), None, NoSpan), false, span)),
        None,
        span
      ),
      span
    )
  )

  override lazy val relationType: RelationType = SchemaType(
    parent = None,
    typeName = LongType.typeName,
    columnTypes = List(NamedType(Name.termName("count"), LongType))
  )

case class EmptyRelation(span: Span) extends Relation with LeafPlan:
  // Need to override this method so as not to create duplicate case object instances
  override def copyInstance(newArgs: Seq[AnyRef]) = this
  override def toString: String                   = s"EmptyRelation()"
  override def relationType: RelationType         = EmptyRelationType

// This node can be a pivot node for generating a SELECT statement
sealed trait Selection extends GeneralSelection:
  /**
    * Attributes corresponding to SELECT items
    */
  def selectItems: List[Attribute]

// This node can be a pivot node for generating a SELECT statement with aggregation functions
trait AggSelect extends Selection

case class Project(child: Relation, selectItems: List[Attribute], span: Span)
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
case class Transform(child: Relation, transformItems: List[Attribute], span: Span)
    extends UnaryRelation
    with Selection
    with LogSupport:
  override def toString: String = s"Transform[${transformItems.mkString(", ")}](${child})"
  override def selectItems: List[Attribute] = transformItems

  override lazy val relationType: RelationType =
    val inputRelation = child.relationType
    val newColumns    = List.newBuilder[NamedType]
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

case class AddColumnsToRelation(child: Relation, newColumns: List[Attribute], span: Span)
    extends UnaryRelation
    with Selection
    with LogSupport:
  override def toString: String = s"Add[${newColumns.mkString(", ")}](${child})"

  override def selectItems: List[Attribute] =
    List(AllColumns(Wildcard(NoSpan), None, NoSpan)) ++ newColumns

  override lazy val relationType: RelationType =
    val cols = List.newBuilder[NamedType]
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

case class PrependColumnsToRelation(child: Relation, newColumns: List[Attribute], span: Span)
    extends UnaryRelation
    with Selection
    with LogSupport:
  override def toString: String = s"Prepend[${newColumns.mkString(", ")}](${child})"
  override def selectItems: List[Attribute] =
    newColumns ++ List(AllColumns(Wildcard(NoSpan), None, NoSpan))

  override lazy val relationType: RelationType =
    val cols = List.newBuilder[NamedType]
    cols ++=
      newColumns.map { x =>
        NamedType(Name.termName(x.nameExpr.leafName), x.dataType)
      }
    cols ++= inputRelationType.fields
    val pt = ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      cols.result(),
      child.relationType
    )
    pt

case class ExcludeColumnsFromRelation(child: Relation, columnNames: List[NameExpr], span: Span)
    extends Selection
    with LogSupport:
  override def toString: String = s"Drop[${columnNames.mkString(", ")}](${child})"

  private def outputColumns: List[NamedType] = inputRelationType
    .fields
    .filterNot(f => columnNames.exists(_.leafName == f.name.name))

  override def selectItems: List[Attribute] = outputColumns.map { c =>
    val name = NameExpr.fromString(c.toSQLAttributeName, span)
    SingleColumn(NameExpr.EmptyName, name, span)
  }

  override lazy val relationType: RelationType =
    val pt = ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      outputColumns,
      child.relationType
    )
    pt

case class RenameColumnsFromRelation(child: Relation, columnAliases: List[Alias], span: Span)
    extends Selection
    with LogSupport:
  override def toString: String = s"Rename[${columnAliases.mkString(", ")}](${child})"

  val columnMapping: ListMap[TermName, TermName] =
    val renamedColumns = ListMap.newBuilder[TermName, TermName]
    inputRelationType
      .fields
      .foreach { f =>
        columnAliases.find(a => a.expr.asInstanceOf[Identifier].toTermName == f.name) match
          case Some(a) =>
            renamedColumns += f.name -> Name.termName(a.nameExpr.leafName)
          case _ =>
      }
    renamedColumns.result()

  override def selectItems: List[Attribute] = inputRelationType
    .fields
    .map { f =>
      columnMapping.get(f.name) match
        case Some(a) =>
          SingleColumn(
            NameExpr.fromString(a.toSQLAttributeName, span),
            NameExpr.fromString(f.toSQLAttributeName, span),
            span
          )
        case None =>
          SingleColumn(NameExpr.EmptyName, NameExpr.fromString(f.toSQLAttributeName, span), span)
    }

  override lazy val relationType: RelationType =
    val newColumns = List.newBuilder[NamedType]
    // Detect newly added columns
    newColumns ++=
      inputRelationType
        .fields
        .map { f =>
          columnMapping.get(f.name) match
            case Some(a) =>
              NamedType(a, f.dataType)
            case None =>
              f
        }
    val pt = ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      newColumns.result(),
      child.relationType
    )
    pt

end RenameColumnsFromRelation

case class ShiftColumns(
    child: Relation,
    isLeftShift: Boolean,
    shiftItems: List[NameExpr],
    span: Span
) extends Selection:
  override def toString: String = s"Shift[${shiftItems.mkString(", ")}](${child})"

  override def selectItems: List[Attribute] = outputColumns.map { c =>
    val name = NameExpr.fromString(c.toSQLAttributeName, span)
    SingleColumn(NameExpr.EmptyName, name, span)
  }

  private def outputColumns: List[NamedType] =
    var inputFields: ListMap[String, NamedType] = inputRelationType
      .fields
      .map { f =>
        f.name.name -> f
      }
      .to(ListMap)
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

    // Shift column positions
    if isLeftShift then
      preferredColumns.result() ++ remainingColumns
    else
      remainingColumns ++ preferredColumns.result()

  override lazy val relationType: RelationType = ProjectedType(
    Name.typeName(RelationType.newRelationTypeName),
    outputColumns,
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
case class GroupBy(child: Relation, groupingKeys: List[GroupingKey], span: Span)
    extends UnaryRelation:
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

end GroupBy

/**
  * Agg(Select) is an operation to report grouping keys and aggregation expressions.
  *
  * @param child
  * @param selectItems
  * @param nodeLocation
  */
case class Agg(child: Relation, keys: List[Attribute], aggExprs: List[Attribute], span: Span)
    extends UnaryRelation
    with AggSelect:
  override def selectItems: List[Attribute] = keys ++ aggExprs

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
    span: Span
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

case class PivotKey(name: Identifier, values: List[Literal], span: Span) extends Expression:
  override def children: List[Expression] = values

case class Unpivot(child: Relation, unpivotKey: UnpivotKey, span: Span) extends UnaryRelation:
  override def toString = s"Unpivot(on:${unpivotKey},${child})"

  override lazy val relationType: RelationType =
    val inputFields      = child.relationType.fields
    val unpivotedColumns = unpivotKey.targetColumns.map(_.fullName)

    val nonPivotColumns = inputFields.filterNot(f => unpivotedColumns.contains(f.name.name))
    val unpivotColumnType: DataType = inputFields
      .find(f => f.name.name == unpivotKey.valueColumnName.fullName)
      .map(_.dataType)
      .getOrElse(DataType.UnknownType)

    ProjectedType(
      Name.typeName(RelationType.newRelationTypeName),
      nonPivotColumns ++
        List(
          NamedType(unpivotKey.valueColumnName.toTermName, StringType),
          NamedType(unpivotKey.unpivotColumnName.toTermName, unpivotColumnType)
        ),
      child.relationType
    )

/**
  * unpivot (value column name) for (unpivot column name) for ((target columns),...)
  * @param unpivotColumnName
  * @param valueColumnName
  * @param targetColumns
  * @param span
  */
case class UnpivotKey(
    valueColumnName: Identifier,
    unpivotColumnName: Identifier,
    targetColumns: List[Identifier],
    // includeNulls: Boolean = false,
    span: Span
) extends Expression:
  override def children: List[Expression] = Nil

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
    orderBy: Seq[SortItem],
    limit: Option[Expression],
    span: Span
) extends UnaryRelation
    with Selection:
  override def toString =
    s"AggregateSelect[${groupingKeys.mkString(",")}](Select[${selectItems.mkString(
        ", "
      )}](${child}))"

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

trait TopLevelStatement extends LogicalPlan

trait QueryStatement extends UnaryRelation with TopLevelStatement

case class Query(body: Relation, span: Span) extends QueryStatement with FilteringRelation:
  override def child: Relation = body
  override def children: List[LogicalPlan] =
    val b = List.newBuilder[LogicalPlan]
    b += body
    b.result()

  override def relationType: RelationType = body.relationType

// Joins
case class Join(
    joinType: JoinType,
    left: Relation,
    right: Relation,
    cond: JoinCriteria,
    asof: Boolean,
    span: Span
) extends Relation
    with LogSupport:
  override def nodeName: String = joinType.toString

  override def children: List[LogicalPlan] = List(left, right)

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
        val mergedColumns = List.newBuilder[NamedType]

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
            List(left.relationType, right.relationType)
          )
        )
      case _ =>
        ConcatType(
          Name.typeName(RelationType.newRelationTypeName),
          List(left.relationType, right.relationType)
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
  override def children: List[Relation]

  def toSQLOp: String
  def toWvOp: String

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

case class Concat(left: Relation, right: Relation, span: Span) extends SetOperation:
  override def toSQLOp: String          = "union all"
  override def toWvOp: String           = "concat"
  override def children: List[Relation] = List(left, right)
  override def toString                 = s"Concat(${left}, ${right})"

case class Dedup(child: Relation, span: Span) extends FilteringRelation:
  override def toString = s"Dedup(${child})"

case class Intersect(left: Relation, right: Relation, isDistinct: Boolean, span: Span)
    extends SetOperation:
  override def toSQLOp: String =
    s"intersect${
        if isDistinct then
          ""
        else
          " all"
      }"

  override def toWvOp: String = toSQLOp

  override def children: List[Relation] = List(left, right)
  override def toString                 = s"Intersect(${left}, ${right})"

case class Except(left: Relation, right: Relation, isDistinct: Boolean, span: Span)
    extends SetOperation:
  override def toSQLOp: String =
    s"except${
        if isDistinct then
          ""
        else
          " all"
      }"

  override def toWvOp: String = toSQLOp

  override def children: List[Relation] = List(left, right)
  override def toString                 = s"Except(${left}, ${right})"

/**
  * Union operation without involving duplicate elimination
  * @param relations
  * @param nodeLocation
  */
case class Union(left: Relation, right: Relation, isDistinct: Boolean, span: Span)
    extends SetOperation:
  override def toSQLOp: String =
    s"union${
        if isDistinct then
          ""
        else
          " all"
      }"

  override def toWvOp: String =
    if isDistinct then
      "union"
    else
      "concat"

  override def children: List[Relation] = List(left, right)
  override def toString                 = s"Union(${children.mkString(",")})"

case class Unnest(columns: Seq[Expression], withOrdinality: Boolean, span: Span) extends Relation:
  override def children: List[LogicalPlan] = Nil

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

case class Lateral(query: Relation, span: Span) extends UnaryRelation:
  override def child: Relation = query

  override lazy val relationType: RelationType =
    // TODO
    UnresolvedRelationType(RelationType.newRelationTypeName)

case class LateralView(
    child: Relation,
    exprs: Seq[Expression],
    tableAlias: NameExpr,
    columnAliases: Seq[NameExpr],
    span: Span
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
case class TableScan(name: TableName, schema: RelationType, columns: List[NamedType], span: Span)
    extends TableInput
    with HasTableName:

  override def sqlExpr: Expression = NameExpr.fromString(name.fullName, span)

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
    span: Span
) extends Relation
    with LeafPlan
    with HasTableName:

  override def relationType: RelationType = ProjectedType(schema.typeName, schema.fields, schema)

  override def toString: String = s"ModelScan(name:${name}, schema:${schema})"

  override lazy val resolved = schema.isResolved

case class Subscribe(
    child: Relation,
    name: Identifier,
    watermarkColumn: Option[String],
    windowSize: Option[String],
    params: Seq[SubscribeParam],
    span: Span
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

case class SubscribeParam(name: String, value: String, span: Span) extends Expression:
  override def children: List[Expression] = Nil

case class IncrementalTableScan(
    name: TableName,
    schema: RelationType,
    columns: List[NamedType],
    span: Span
) extends Relation
    with LeafPlan
    with HasTableName:

  override val relationType: RelationType = ProjectedType(schema.typeName, columns, schema)

  override def toString: String =
    s"IncrementalScan(name:${name}, columns:[${columns.mkString(", ")}])"

case class IncrementalAppend(
    // Differential query
    child: Relation,
    span: Span
) extends UnaryRelation:
  override def relationType: RelationType = child.relationType

enum ShowType:
  case models
  case tables
  case schemas
  case catalogs
  case databases
  case query

case class Show(showType: ShowType, inExpr: NameExpr, span: Span) extends Relation with LeafPlan:
  override def relationType: RelationType =
    showType match
      case ShowType.models =>
        SchemaType(
          parent = None,
          typeName = Name.typeName("model"),
          columnTypes = List[NamedType](
            NamedType(Name.termName("name"), DataType.StringType),
            NamedType(Name.termName("args"), DataType.StringType),
            NamedType(Name.termName("description"), DataType.StringType),
            NamedType(Name.termName("package_name"), DataType.StringType)
          )
        )
      case ShowType.tables =>
        SchemaType(
          parent = None,
          typeName = Name.typeName("table"),
          columnTypes = List[NamedType](NamedType(Name.termName("name"), DataType.StringType))
        )
      case ShowType.schemas =>
        SchemaType(
          parent = None,
          typeName = Name.typeName("schema"),
          columnTypes = List[NamedType](
            NamedType(Name.termName("catalog"), DataType.StringType),
            NamedType(Name.termName("name"), DataType.StringType)
          )
        )
      case ShowType.catalogs =>
        SchemaType(
          parent = None,
          typeName = Name.typeName("catalog"),
          columnTypes = List[NamedType](NamedType(Name.termName("name"), DataType.StringType))
        )
      case other =>
        throw StatusCode.UNEXPECTED_STATE.newException(s"Unexpected show type: ${other}")

end Show

trait RelationInspector extends QueryStatement

case class Describe(child: Relation, span: Span) extends RelationInspector:
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

case class Sample(child: Relation, method: Option[SamplingMethod], size: SamplingSize, span: Span)
    extends FilteringRelation:
  override def relationType: RelationType = child.relationType

enum SamplingMethod:
  case reservoir
  case system
  case bernoulli

enum SamplingSize:
  def toExpr: String =
    this match
      case Rows(rows) =>
        rows.toString
      case Percentage(p) =>
        s"${p}%"

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
case class Debug(child: Relation, debugExpr: Relation, span: Span) extends FilteringRelation:
  /**
    * A partial debug expression that only contains operators inside the debug expression
    * @return
    */
  def partialDebugExpr: Relation = debugExpr
    .transformUp { case l: LeafPlan =>
      EmptyRelation(l.span)
    }
    .asInstanceOf[Relation]

  // Add debug expr as well for the ease of tree traversal
  override def children: List[LogicalPlan] = List(child, debugExpr)

/**
  * A relation that contains a query body with query definitions.
  *
  * @param isRecursive
  * @param queryDefs
  * @param queryBody
  * @param span
  */
case class WithQuery(
    isRecursive: Boolean,
    queryDefs: List[AliasedRelation],
    queryBody: Relation,
    span: Span
) extends Relation:
  override def children: List[LogicalPlan] =
    // Technically, queryDefs are not the children, rather prerequisites of the queryBody
    Nil

  override def relationType: RelationType = queryBody.relationType

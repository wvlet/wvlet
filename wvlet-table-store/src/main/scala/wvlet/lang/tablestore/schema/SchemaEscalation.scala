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
package wvlet.lang.tablestore.schema

import wvlet.lang.tablestore.{EntryId, SchemaVersionId, TableStoreException}
import wvlet.uni.json.JSON

/** One column of a published (escalated) schema */
case class ColumnDesc(name: String, columnType: ColumnType):
  def schemaJson: String =
    (
      JSON.JSONObject(
        Seq(
          "name" -> JSON.JSONString(name),
          "type" -> JSON.JSONString(ObservedSchema.typeName(columnType))
        )
      )
    ).toJSON

object ColumnDesc:
  def fromJson(v: JSON.JSONObject): ColumnDesc =
    val name = v
      .get("name")
      .collect { case JSON.JSONString(s) =>
        s
      }
    val typ = v
      .get("type")
      .collect { case JSON.JSONString(s) =>
        s
      }
    (name, typ) match
      case (Some(n), Some(t)) =>
        ColumnDesc(n, ColumnType.parse(t))
      case _ =>
        throw TableStoreException(s"Malformed column descriptor: ${v.toJSON}")

/** A published table schema: deterministic, name-sorted column definitions */
case class TableSchema(version: SchemaVersionId, columns: Seq[ColumnDesc]):
  def column(name: String): Option[ColumnDesc] = columns.find(_.name == name)
  def schemaJson: String                       =
    val cols = columns.map { c =>
      JSON.JSONObject(
        Seq(
          "name" -> JSON.JSONString(c.name),
          "type" -> JSON.JSONString(ObservedSchema.typeName(c.columnType))
        )
      )
    }
    (JSON.JSONObject(Seq("columns" -> JSON.JSONArray(cols.toIndexedSeq)))).toJSON

object TableSchema:
  val empty: TableSchema = TableSchema(0L, Nil)

  def fromJson(version: SchemaVersionId, json: String): TableSchema =
    if json == null || json.isEmpty then
      TableSchema(version, Nil)
    else
      JSON.parse(json) match
        case obj: JSON.JSONObject =>
          obj.get("columns") match
            case Some(JSON.JSONArray(items)) =>
              TableSchema(
                version,
                items.collect { case c: JSON.JSONObject =>
                  ColumnDesc.fromJson(c)
                }
              )
            case _ =>
              TableSchema(version, Nil)
        case _ =>
          throw TableStoreException("Malformed table schema JSON")

/**
  * Result of escalating one table's schema over a batch of files.
  *
  * @param escalatedSchema
  *   the new published schema if any column widened or files were quarantined; None when the schema
  *   head already covers everything observed
  * @param quarantinedFiles
  *   files excluded by the outlier guardrail. They remain registered and readable; their columns
  *   re-enter escalation only after review
  */
case class EscalationResult(
    currentSchema: TableSchema,
    escalatedSchema: Option[TableSchema],
    quarantinedFiles: Seq[EntryId]
)

/**
  * Merge-time schema escalation following PlazmaDB's lazy escalation, made deterministic:
  *   - folding observed schemas is order-independent ([[ColumnType.lub]] is a join-semilattice);
  *   - the outlier guardrail quarantines a widening forced by fewer than the threshold fraction of
  *     rows instead of irreversibly escalating.
  *
  * The guardrail runs per column in pass one; every file flagged for any column is then excluded
  * entirely and the fold recomputes over the survivors in pass two.
  */
object SchemaEscalation:

  /** A widening forced by less than this fraction of rows quarantines the offending file */
  val defaultOutlierThreshold: Double = 0.001

  /**
    * @param files
    *   (file id, row count, observed schema) triples of the entries being merged
    * @return
    *   the escalated schema and the files quarantined by the guardrail
    */
  def escalate(
      currentSchema: TableSchema,
      nextVersion: SchemaVersionId,
      files: Seq[(EntryId, Long, ObservedSchema)],
      outlierThreshold: Double = defaultOutlierThreshold
  ): EscalationResult =
    val outliers = detectOutliers(currentSchema, files, outlierThreshold)
    val kept     = files.filterNot((id, _, _) => outliers(id))

    val allColumns = (currentSchema.columns.map(_.name) ++ kept.flatMap(_._3.columns.map(_._1)))
      .distinct
      .sorted

    val newColumns = allColumns.map { col =>
      val contributors = kept.flatMap { (id, rows, observed) =>
        observed.columnType(col).map(t => (id, rows, t))
      }
      val totalRows = contributors.map(_._2).sum
      val folded    =
        totalRows match
          case 0L =>
            // Only the published schema mentions this column (or nothing at all)
            currentSchema.column(col).map(_.columnType).getOrElse(ColumnType.NullType)
          case _ =>
            ColumnType.fold(contributors.map(_._3))
      // Escalation never narrows: keep the published type when observations folded lower
      val resolvedType =
        currentSchema.column(col).map(_.columnType) match
          case Some(published) =>
            ColumnType.lub(published, folded)
          case None =>
            folded
      ColumnDesc(col, resolvedType)
    }

    val widened = newColumns.exists { col =>
      currentSchema.column(col.name) match
        // A brand-new column with an observed type widens the schema
        case None =>
          col.columnType != ColumnType.NullType
        case Some(prev) =>
          ColumnType.lub(prev.columnType, col.columnType) != prev.columnType
    }

    val escalated =
      if widened then
        Some(TableSchema(nextVersion, newColumns))
      else
        None

    EscalationResult(currentSchema, escalated, outliers.toSeq.sorted)
  end escalate

  /**
    * Files whose removal would avoid a widening while dropping fewer than `outlierThreshold` of the
    * batch's rows. Two rules, both evaluated per column:
    *   - minority introduction: a column carried by fewer than the threshold of all batch rows
    *     quarantines every file that introduces it (a widening out of null is still irreversible);
    *   - single-file forcing: if dropping one file lowers the union type, and its rows are under
    *     the threshold, quarantine it.
    *
    * Deterministic: folds are order-independent and files are visited in id order.
    */
  private def detectOutliers(
      currentSchema: TableSchema,
      files: Seq[(EntryId, Long, ObservedSchema)],
      outlierThreshold: Double
  ): Set[EntryId] =
    val batchRows = files.map(_._2).sum.toDouble
    val columns   = files.flatMap(_._3.columns.map(_._1)).distinct.sorted

    val flagged =
      for
        col          <- columns
        contributors <- Seq(files.flatMap(f => f._3.columnType(col).map(t => (f._1, f._2, t))))
        contributorRows = contributors.map(_._2).sum
        unionType       = ColumnType.fold(contributors.map(_._3))
        published       = currentSchema.column(col).map(_.columnType)
        // No guardrail when the fold matches what is already published — republishing needs no
        // justification. A column absent from the catalog has no widening to protect either.
        if !published.contains(unionType) && unionType != ColumnType.NullType && batchRows > 0
        candidate <-
          if contributorRows.toDouble / batchRows < outlierThreshold then
            // Rule 1: the whole introducing minority goes to review
            contributors
          else
            // Rule 2: drop one file at a time and see whether the union drops with it
            for
              c <- contributors
              remaining = contributors.filterNot(_._1 == c._1)
              if remaining.nonEmpty
              keptType = ColumnType.fold(remaining.map(_._3))
              // Only consider removals that actually lower the union type
              if keptType != unionType && ColumnType.lub(keptType, unionType) == unionType
              droppedFraction = c._2.toDouble / batchRows.toDouble
              if droppedFraction < outlierThreshold
            yield c
      yield candidate._1

    flagged.toSet
  end detectOutliers

end SchemaEscalation

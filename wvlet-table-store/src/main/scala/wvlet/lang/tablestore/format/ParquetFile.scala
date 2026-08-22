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
package wvlet.lang.tablestore.format

import wvlet.lang.tablestore.{DataRow, TableStoreException}
import wvlet.lang.tablestore.schema.{ColumnType, TableSchema}
import wvlet.uni.json.JSON

import java.sql.DriverManager

/**
  * Produces read-optimized Parquet files for merged entries. Column types are cast explicitly from
  * the escalated schema — production writers never rely on engine-side schema inference.
  */
object ParquetFile:

  // JDBC 4 auto-discovery does not always survive test classloaders
  private lazy val driverLoaded = Class.forName("org.duckdb.DuckDBDriver")

  private def newConnection: java.sql.Connection =
    driverLoaded
    DriverManager.getConnection("jdbc:duckdb:")

  private def duckDbType(t: ColumnType): String =
    t match
      case ColumnType.NullType =>
        "VARCHAR" // all-null columns stay nullable strings
      case ColumnType.BooleanType =>
        "BOOLEAN"
      case ColumnType.LongType =>
        "BIGINT"
      case ColumnType.DoubleType =>
        "DOUBLE"
      case ColumnType.StringType =>
        "VARCHAR"

  /**
    * Write rows as a Parquet file with exactly the columns of `schema`. Rows may carry extra or
    * missing columns; extras are dropped and missing ones become NULL.
    */
  def write(rows: Seq[DataRow], schema: TableSchema, outputPath: String): Unit =
    if schema.columns.isEmpty then
      throw TableStoreException("Refusing to write a Parquet file with no columns")
    val normalized = normalize(rows, schema)
    val tmpJsonl   = java.nio.file.Files.createTempFile("wvlet-merge-", ".jsonl")
    try
      JsonlFile.write(tmpJsonl.toString, normalized)
      val conn = newConnection
      try
        val st = conn.createStatement()
        try
          val colDefs = schema
            .columns
            .map(c => s"'${escapeSql(c.name)}': '${duckDbType(c.columnType)}'")
            .mkString(", ")
          // Explicit column spec — no engine-side inference
          st.execute(
            s"""CREATE TABLE merged AS SELECT ${selectList(schema)} FROM read_json('${escapeSql(
                tmpJsonl.toString
              )}', columns={${colDefs}})"""
          )
          st.execute(s"COPY merged TO '${escapeSql(outputPath)}' (FORMAT PARQUET)")
        finally
          st.close()
      finally
        conn.close()
    finally
      java.nio.file.Files.deleteIfExists(tmpJsonl)
  end write

  private def selectList(schema: TableSchema): String = schema
    .columns
    .map { c =>
      c.columnType match
        case ColumnType.LongType =>
          s"""CAST("${quote(c.name)}" AS BIGINT) AS "${quote(c.name)}""""
        case ColumnType.DoubleType =>
          s"""CAST("${quote(c.name)}" AS DOUBLE) AS "${quote(c.name)}""""
        case ColumnType.StringType =>
          s"""CAST("${quote(c.name)}" AS VARCHAR) AS "${quote(c.name)}""""
        case ColumnType.BooleanType =>
          s"""CAST("${quote(c.name)}" AS BOOLEAN) AS "${quote(c.name)}""""
        case ColumnType.NullType =>
          s"""CAST(NULL AS VARCHAR) AS "${quote(c.name)}""""
    }
    .mkString(", ")

  /** Cast one row to the target schema, filling missing columns with null */
  def normalizeRow(row: DataRow, schema: TableSchema): DataRow = JSON.JSONObject(
    schema
      .columns
      .map { col =>
        val v: JSON.JSONValue =
          row.get(col.name) match
            case None | Some(_: JSON.JSONNull) =>
              JSON.JSONNull()
            case Some(raw) =>
              (col.columnType, raw) match
                case (ColumnType.NullType, _) =>
                  raw
                case (ColumnType.LongType, l: JSON.JSONLong) =>
                  l
                case (ColumnType.LongType, d: JSON.JSONDouble) =>
                  JSON.JSONLong(d.v.toLong)
                case (ColumnType.LongType, JSON.JSONString(s)) =>
                  JSON.JSONLong(s.toLong)
                case (ColumnType.DoubleType, d: JSON.JSONDouble) =>
                  d
                case (ColumnType.DoubleType, l: JSON.JSONLong) =>
                  JSON.JSONDouble(l.v.toDouble)
                case (ColumnType.DoubleType, JSON.JSONString(s)) =>
                  JSON.JSONDouble(s.toDouble)
                case (ColumnType.StringType, s: JSON.JSONString) =>
                  s
                case (ColumnType.StringType, structured: (JSON.JSONArray | JSON.JSONObject)) =>
                  JSON.JSONString(structured.toJSON)
                case (ColumnType.StringType, other) =>
                  JSON.JSONString(otherValueString(other))
                case (ColumnType.BooleanType, b: JSON.JSONBoolean) =>
                  b
                case (ColumnType.BooleanType, _) =>
                  throw TableStoreException(s"Cannot coerce value to boolean: ${JSON.format(raw)}")
        col.name -> v
      }
  )

  private def normalize(rows: Seq[DataRow], schema: TableSchema): Seq[DataRow] = rows.map(
    normalizeRow(_, schema)
  )

  private def otherValueString(v: JSON.JSONValue): String =
    v match
      case JSON.JSONBoolean(b) =>
        b.toString
      case n: JSON.JSONNumber =>
        JSON.format(n)
      case other =>
        JSON.format(other)

  private def escapeSql(s: String): String = s.replace("'", "\\'")

  private def quote(s: String): String = s.replace("\"", "\\\"")

  /** Read a Parquet file into typed JSON rows (JSONLong / JSONDouble / JSONString / JSONBoolean) */
  def read(path: String): Seq[DataRow] =
    val conn = newConnection
    try
      val st = conn.createStatement()
      try
        val rs = st.executeQuery(s"SELECT * FROM read_parquet('${escapeSql(path)}')")
        try
          val meta  = rs.getMetaData
          val n     = meta.getColumnCount
          val names = (1 to n).map(meta.getColumnLabel)
          val b     = Seq.newBuilder[DataRow]
          while rs.next() do
            b +=
              JSON.JSONObject(
                (names.zipWithIndex).map { (name, i) =>
                  name -> valueAt(rs, i + 1, meta.getColumnType(i + 1))
                }
              )
          b.result()
        finally
          rs.close()
      finally
        st.close()
    finally
      conn.close()

  private def valueAt(rs: java.sql.ResultSet, index: Int, sqlType: Int): JSON.JSONValue =
    import java.sql.Types.*
    sqlType match
      case TINYINT | SMALLINT | INTEGER | BIGINT =>
        val v = rs.getLong(index)
        if rs.wasNull then
          JSON.JSONNull()
        else
          JSON.JSONLong(v)
      case FLOAT | DOUBLE | REAL | DECIMAL | NUMERIC =>
        val v = rs.getDouble(index)
        if rs.wasNull then
          JSON.JSONNull()
        else
          JSON.JSONDouble(v)
      case BOOLEAN =>
        val v = rs.getBoolean(index)
        if rs.wasNull then
          JSON.JSONNull()
        else
          JSON.JSONBoolean(v)
      case _ =>
        val v = rs.getString(index)
        if rs.wasNull then
          JSON.JSONNull()
        else
          JSON.JSONString(v)

end ParquetFile

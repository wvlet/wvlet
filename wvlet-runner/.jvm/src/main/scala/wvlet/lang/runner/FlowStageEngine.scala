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
package wvlet.lang.runner

import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.connector.CancellableStatement
import wvlet.lang.connector.Connector
import wvlet.lang.connector.DBConnector
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.DoubleQuotedIdentifier
import wvlet.lang.model.plan.Relation
import wvlet.lang.model.plan.Save
import wvlet.lang.model.plan.TableRef
import wvlet.lang.runner.engine.WvletEngineConnector
import wvlet.lang.runner.engine.WvletEngineSession
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONNull
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/**
  * Hooks registering the running statement of a stage attempt, so that the scheduler can cancel it
  * on a timeout and count it as alive for heartbeats
  */
private[runner] case class StatementHooks(
    register: CancellableStatement => Unit,
    deregister: () => Unit
)

private[runner] object StatementHooks:
  val none: StatementHooks = StatementHooks(_ => (), () => ())

/**
  * The engine a flow stage runs on: the operations [[FlowExecutor]] needs to materialize stage
  * results, independent of whether the engine runs SQL text (a [[DBConnector]]) or Wvlet plans (the
  * in-process wvlet engine)
  */
private[runner] trait FlowStageEngine:
  /** The connector backing this engine */
  def connector: Connector

  def dbType: DBType

  /** The SQL connector of this engine, if it runs SQL */
  def sqlConnector: Option[DBConnector]

  /** Create or replace the table with the rows of the query */
  def createTableAs(table: String, query: Relation, hooks: StatementHooks)(using Context): Unit

  /** Run a save statement (`save to`, `append to`, `save as view`, ...) */
  def save(save: Save, hooks: StatementHooks)(using Context): Unit

  /** Drop the table if it exists */
  def dropTable(table: String): Unit

  /**
    * Create or replace the table with the given columns and rows. Values are String, Int, Long, or
    * null; columns are StringType, IntType, or LongType
    */
  def replaceTableWithRows(table: String, columns: List[(String, DataType)], rows: List[List[Any]])(
      using Context
  ): Unit

  /** Whether the query returns at least one row */
  def hasRows(query: Relation)(using Context): Boolean

  /** The values of a column of the table, as strings (None for NULL) */
  def columnValues(table: String, column: String)(using Context): List[Option[String]]

  /** Whether the table exists and is readable */
  def tableExists(table: String): Boolean

end FlowStageEngine

private[runner] object FlowStageEngine:

  /**
    * The stage engine of a connector: SQL for a [[DBConnector]], plans for the in-process wvlet
    * engine. Other connectors (e.g. Slack) cannot run stages
    */
  def of(connector: Connector): FlowStageEngine =
    connector match
      case db: DBConnector =>
        SqlStageEngine(db)
      case w: WvletEngineConnector =>
        WvletStageEngine(w)
      case other =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"Connector '${other.name}' (${other
                .connectorType}) is not a SQL engine and cannot run a stage"
          )

  /** Fail when a feature is not available on the wvlet engine yet */
  def notOnEngine(feature: String): Nothing =
    throw StatusCode
      .NOT_IMPLEMENTED
      .newException(
        s"${feature} is not supported on the wvlet engine yet: run the flow on a SQL engine " +
          "(e.g. -t duckdb)"
      )

end FlowStageEngine

/** Stages as SQL statements on a [[DBConnector]] */
private[runner] class SqlStageEngine(db: DBConnector) extends FlowStageEngine with LogSupport:
  override def connector: Connector              = db
  override def dbType: DBType                    = db.dbType
  override def sqlConnector: Option[DBConnector] = Some(db)

  private def quoted(table: String): String = s""""${table}""""

  // Engines without `create or replace table` (e.g. Trino) get an explicit drop first
  private def createOrReplaceDDL(table: String, select: String): String =
    if db.dbType.supportCreateOrReplace then
      s"create or replace table ${quoted(table)} as\n${select}"
    else
      db.execute(s"drop table if exists ${quoted(table)}")
      s"create table ${quoted(table)} as\n${select}"

  override def createTableAs(table: String, query: Relation, hooks: StatementHooks)(using
      Context
  ): Unit =
    val sql = GenSQL.generateSQLFromRelation(query, addHeader = false).sql
    val ddl = createOrReplaceDDL(table, sql)
    debug(s"Materializing ${table}:\n${ddl}")
    db.executeCancellable(ddl, hooks.register, hooks.deregister)

  override def save(save: Save, hooks: StatementHooks)(using ctx: Context): Unit = GenSQL
    .generateSaveSQL(save, ctx)
    .foreach { stmt =>
      debug(s"Saving:\n${stmt}")
      db.executeCancellable(stmt, hooks.register, hooks.deregister)
    }

  override def dropTable(table: String): Unit = db.execute(s"drop table if exists ${quoted(table)}")

  private def sqlTypeOf(t: DataType): String =
    t match
      case DataType.IntType =>
        "integer"
      case DataType.LongType =>
        "bigint"
      case _ =>
        "varchar"

  override def replaceTableWithRows(
      table: String,
      columns: List[(String, DataType)],
      rows: List[List[Any]]
  )(using Context): Unit =
    def lit(v: Any): String =
      v match
        case null =>
          "null"
        case n: (Int | Long) =>
          n.toString
        case other =>
          s"'${other.toString.replace("'", "''")}'"
    val names = columns.map(_._1)
    // An empty table still produces the schema
    val values =
      if rows.isEmpty then
        List(names.map(_ => "null").mkString("(", ", ", ")"))
      else
        rows.map(_.map(lit).mkString("(", ", ", ")"))
    val emptyFilter =
      if rows.isEmpty then
        "\nwhere 1 = 0"
      else
        ""
    // Built without stripMargin: values may contain margin characters
    val select = List(
      "select " + columns.map((n, t) => s"cast(${n} as ${sqlTypeOf(t)}) as ${n}").mkString(", "),
      s"from (values\n  ${values.mkString(",\n  ")}\n) as t(${names.mkString(", ")})${emptyFilter}"
    ).mkString("\n")
    db.execute(createOrReplaceDDL(table, select))

  end replaceTableWithRows

  override def hasRows(query: Relation)(using Context): Boolean =
    val sql = GenSQL.generateSQLFromRelation(query, addHeader = false).sql
    db.queryJsonRows(s"""select 1 from (\n${sql}\n) as "__wv_sensor" limit 1""").nonEmpty

  override def columnValues(table: String, column: String)(using Context): List[Option[String]] = db
    .queryJsonRows(s"""select "${column}" from ${quoted(table)}""")
    .map { row =>
      JSON.parse(row) match
        case o: JSONObject =>
          o.get(column) match
            case Some(JSONString(s)) =>
              Some(s)
            case Some(JSONNull) | None =>
              None
            case Some(other) =>
              Some(other.toJSON)
        case _ =>
          None
    }

  override def tableExists(table: String): Boolean =
    try
      db.execute(s"select 1 from ${table} limit 0")
      true
    catch
      case NonFatal(e) =>
        debug(s"${table} is not readable: ${e.getMessage}")
        false

end SqlStageEngine

/**
  * Stages as Wvlet plans on the in-process wvlet engine. Statements run one at a time on the
  * session, and can't be cancelled mid-statement
  */
private[runner] class WvletStageEngine(engineConnector: WvletEngineConnector)
    extends FlowStageEngine:
  private def session: WvletEngineSession = engineConnector.session

  override def connector: Connector              = engineConnector
  override def dbType: DBType                    = DBType.Wvlet
  override def sqlConnector: Option[DBConnector] = None

  // Parallel stages share one session; its catalog and writer aren't made for concurrent writes
  private def locked[A](body: => A): A = session.synchronized(body)

  // A no-op handle keeps the attempt counted as alive while the statement runs
  private def withHooks[A](hooks: StatementHooks)(body: => A): A =
    hooks.register(() => ())
    try body
    finally hooks.deregister()

  private def tableRef(table: String): Relation = TableRef(
    DoubleQuotedIdentifier(table, Span.NoSpan),
    Span.NoSpan
  )

  override def createTableAs(table: String, query: Relation, hooks: StatementHooks)(using
      Context
  ): Unit = withHooks(hooks)(locked(session.createTableAs(table, query)))

  override def save(save: Save, hooks: StatementHooks)(using Context): Unit =
    withHooks(hooks)(locked(session.save(save)))

  override def dropTable(table: String): Unit = locked(session.dropTable(table))

  override def replaceTableWithRows(
      table: String,
      columns: List[(String, DataType)],
      rows: List[List[Any]]
  )(using Context): Unit = locked(session.replaceTableWithRows(table, columns, rows))

  override def hasRows(query: Relation)(using Context): Boolean = locked(
    session.execute(query, rowLimit = 1).rows.nonEmpty
  )

  override def columnValues(table: String, column: String)(using Context): List[Option[String]] =
    locked(
      session
        .execute(tableRef(table), rowLimit = Int.MaxValue)
        .rows
        .map(row => row.get(column).flatMap(Option(_)).map(_.toString))
        .toList
    )

  override def tableExists(table: String): Boolean = locked(session.hasTable(table))

end WvletStageEngine

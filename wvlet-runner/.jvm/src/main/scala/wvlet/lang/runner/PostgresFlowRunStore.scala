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

import wvlet.lang.api.StatusCode
import wvlet.uni.log.LogSupport
import wvlet.uni.weaver.Weaver

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import scala.util.Using
import scala.util.control.NonFatal

/**
  * A PostgreSQL-backed flow run store, sharing run records across machines: flow-level
  * `concurrency:` limits, scheduler catch-up, and the web UI observe runs recorded by any process
  * pointing at the same database.
  *
  * The schema mirrors [[SQLiteFlowRunStore]] (`runs` + `stages` with a JSON `args` column). Unlike
  * SQLite, PostgreSQL does not serialize writers, so [[claimRunSlot]] takes a transaction-scoped
  * advisory lock keyed on the flow name before its guarded insert — two concurrent claims of the
  * same flow are evaluated one after the other, on any number of hosts.
  *
  * The store keeps one connection per instance; all operations are synchronized on it
  */
class PostgresFlowRunStore(jdbcUrl: String, user: String, password: String)
    extends FlowRunStore
    with LogSupport:

  import SQLiteFlowRunStore.RunArgs

  // Bound flow arguments are stored as a single JSON object column
  private val argsWeaver = Weaver.of[RunArgs]

  private val conn: Connection =
    val props = Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    DriverManager.getConnection(jdbcUrl, props)

  Using.resource(conn.createStatement()) { stmt =>
    stmt.execute("""create table if not exists runs(
        |  run_id           text primary key,
        |  flow_name        text not null,
        |  state            text not null,
        |  started_at       bigint not null,
        |  finished_at      bigint,
        |  cancel_requested integer not null default 0,
        |  lease_expires_at bigint,
        |  args             text,
        |  run_time         bigint
        |)""".stripMargin)
    stmt.execute("""create table if not exists stages(
        |  run_id        text not null,
        |  ordinal       integer not null,
        |  name          text not null,
        |  state         text not null,
        |  attempts      integer not null,
        |  error         text,
        |  table_name    text,
        |  waiting_since bigint,
        |  last_poll_at  bigint,
        |  primary key(run_id, ordinal)
        |)""".stripMargin)
  }

  override def save(record: FlowRunRecord): Unit = synchronized {
    inTransaction {
      Using.resource(
        conn.prepareStatement(
          """insert into runs(run_id, flow_name, state, started_at, finished_at, lease_expires_at, args, run_time)
            |values(?, ?, ?, ?, ?, ?, ?, ?)
            |on conflict(run_id) do update set
            |  flow_name = excluded.flow_name,
            |  state = excluded.state,
            |  started_at = excluded.started_at,
            |  finished_at = excluded.finished_at,
            |  lease_expires_at = excluded.lease_expires_at,
            |  args = excluded.args,
            |  run_time = excluded.run_time""".stripMargin
        )
      ) { ps =>
        bindRunColumns(ps, record)
        ps.executeUpdate()
      }
      saveStages(record)
    }
  }

  private def inTransaction[A](body: => A): A =
    conn.setAutoCommit(false)
    try
      val result = body
      conn.commit()
      result
    catch
      case NonFatal(e) =>
        conn.rollback()
        throw e
    finally
      conn.setAutoCommit(true)

  private def bindRunColumns(ps: java.sql.PreparedStatement, record: FlowRunRecord): Unit =
    ps.setString(1, record.runId.toLowerCase)
    ps.setString(2, record.flowName)
    ps.setString(3, record.state)
    ps.setLong(4, record.startedAtMillis)
    record.finishedAtMillis match
      case Some(f) =>
        ps.setLong(5, f)
      case None =>
        ps.setNull(5, java.sql.Types.BIGINT)
    record.leaseExpiresAtMillis match
      case Some(l) =>
        ps.setLong(6, l)
      case None =>
        ps.setNull(6, java.sql.Types.BIGINT)
    if record.args.isEmpty then
      ps.setNull(7, java.sql.Types.VARCHAR)
    else
      ps.setString(7, argsWeaver.toJson(RunArgs(record.args)))
    record.runTimeMillis match
      case Some(t) =>
        ps.setLong(8, t)
      case None =>
        ps.setNull(8, java.sql.Types.BIGINT)

  private def saveStages(record: FlowRunRecord): Unit =
    Using.resource(conn.prepareStatement("delete from stages where run_id = ?")) { ps =>
      ps.setString(1, record.runId.toLowerCase)
      ps.executeUpdate()
    }
    Using.resource(
      conn.prepareStatement(
        "insert into stages(run_id, ordinal, name, state, attempts, error, table_name, waiting_since, last_poll_at) values(?, ?, ?, ?, ?, ?, ?, ?, ?)"
      )
    ) { ps =>
      def setLongOpt(index: Int, value: Option[Long]): Unit =
        value match
          case Some(v) =>
            ps.setLong(index, v)
          case None =>
            ps.setNull(index, java.sql.Types.BIGINT)
      record
        .stages
        .zipWithIndex
        .foreach { (s, i) =>
          ps.setString(1, record.runId.toLowerCase)
          ps.setInt(2, i)
          ps.setString(3, s.name)
          ps.setString(4, s.state)
          ps.setInt(5, s.attempts)
          ps.setString(6, s.error.orNull)
          ps.setString(7, s.table.orNull)
          setLongOpt(8, s.waitingSinceMillis)
          setLongOpt(9, s.lastPollAtMillis)
          ps.addBatch()
        }
      ps.executeBatch()
    }

  end saveStages

  override def get(runId: String): Option[FlowRunRecord] = synchronized {
    queryRuns("where run_id = ?", _.setString(1, runId.toLowerCase)).headOption
  }

  override def list(): List[FlowRunRecord] = synchronized {
    queryRuns("order by started_at desc, run_id desc", _ => ())
  }

  override def claimRunSlot(record: FlowRunRecord, concurrencyLimit: Int): Boolean = synchronized {
    // Serialize claims of the same flow across processes with a transaction-scoped advisory
    // lock: unlike SQLite, PostgreSQL evaluates the count subquery against a snapshot, so two
    // concurrent guarded inserts could otherwise both pass the check. Running records whose
    // liveness lease has expired belong to dead processes and do not occupy a slot
    inTransaction {
      Using.resource(conn.prepareStatement("select pg_advisory_xact_lock(hashtext(?))")) { ps =>
        ps.setString(1, record.flowName)
        ps.execute()
      }
      val claimed =
        Using.resource(
          conn.prepareStatement(
            """insert into runs(run_id, flow_name, state, started_at, finished_at, lease_expires_at, args, run_time)
              |select ?, ?, ?, ?, ?, ?, ?, ?
              |where (select count(*) from runs
              |       where flow_name = ? and state = ?
              |         and (lease_expires_at is null or lease_expires_at >= ?)) < ?""".stripMargin
          )
        ) { ps =>
          bindRunColumns(ps, record)
          ps.setString(9, record.flowName)
          ps.setString(10, FlowRunRecord.STATE_RUNNING)
          ps.setLong(11, System.currentTimeMillis())
          ps.setInt(12, concurrencyLimit)
          ps.executeUpdate() == 1
        }
      if claimed then
        saveStages(record)
      claimed
    }
  }

  override def refreshLease(runId: String, leaseExpiresAtMillis: Long): Unit = synchronized {
    Using.resource(conn.prepareStatement("update runs set lease_expires_at = ? where run_id = ?")) {
      ps =>
        ps.setLong(1, leaseExpiresAtMillis)
        ps.setString(2, runId.toLowerCase)
        ps.executeUpdate()
    }
  }

  override def requestCancel(runId: String): Unit = synchronized {
    setCancelRequested(runId, requested = true)
  }

  override def cancelRequested(runId: String): Boolean = synchronized {
    Using.resource(conn.prepareStatement("select cancel_requested from runs where run_id = ?")) {
      ps =>
        ps.setString(1, runId.toLowerCase)
        Using.resource(ps.executeQuery()) { rs =>
          rs.next() && rs.getInt(1) != 0
        }
    }
  }

  override def clearCancelRequest(runId: String): Unit = synchronized {
    setCancelRequested(runId, requested = false)
  }

  private def setCancelRequested(runId: String, requested: Boolean): Unit =
    Using.resource(conn.prepareStatement("update runs set cancel_requested = ? where run_id = ?")) {
      ps =>
        ps.setInt(
          1,
          if requested then
            1
          else
            0
        )
        ps.setString(2, runId.toLowerCase)
        ps.executeUpdate()
    }

  override def delete(runId: String): Unit = synchronized {
    Using.resource(conn.prepareStatement("delete from stages where run_id = ?")) { ps =>
      ps.setString(1, runId.toLowerCase)
      ps.executeUpdate()
    }
    Using.resource(conn.prepareStatement("delete from runs where run_id = ?")) { ps =>
      ps.setString(1, runId.toLowerCase)
      ps.executeUpdate()
    }
  }

  override def close(): Unit = synchronized {
    conn.close()
  }

  private def queryRuns(
      clause: String,
      bind: java.sql.PreparedStatement => Unit
  ): List[FlowRunRecord] =
    val runs =
      Using.resource(
        conn.prepareStatement(
          s"select run_id, flow_name, state, started_at, finished_at, lease_expires_at, args, run_time from runs ${clause}"
        )
      ) { ps =>
        bind(ps)
        Using.resource(ps.executeQuery()) { rs =>
          // wasNull refers to the immediately preceding column read
          def nullableLong(column: Int): Option[Long] =
            val v = rs.getLong(column)
            if rs.wasNull() then
              None
            else
              Some(v)
          val b = List.newBuilder[FlowRunRecord]
          while rs.next() do
            val finishedAtOpt = nullableLong(5)
            val leaseOpt      = nullableLong(6)
            val argsOpt       = Option(rs.getString(7))
            val runTimeOpt    = nullableLong(8)
            b +=
              FlowRunRecord(
                runId = rs.getString(1),
                flowName = rs.getString(2),
                state = rs.getString(3),
                startedAtMillis = rs.getLong(4),
                finishedAtMillis = finishedAtOpt,
                leaseExpiresAtMillis = leaseOpt,
                args = argsOpt.map(json => argsWeaver.fromJson(json).args).getOrElse(Map.empty),
                runTimeMillis = runTimeOpt
              )
          b.result()
        }
      }
    runs.map(r => r.copy(stages = stagesOf(r.runId)))

  end queryRuns

  private def stagesOf(runId: String): List[StageRunRecord] =
    Using.resource(
      conn.prepareStatement(
        "select name, state, attempts, error, table_name, waiting_since, last_poll_at from stages where run_id = ? order by ordinal"
      )
    ) { ps =>
      ps.setString(1, runId.toLowerCase)
      Using.resource(ps.executeQuery()) { rs =>
        def getLongOpt(index: Int): Option[Long] =
          val v = rs.getLong(index)
          if rs.wasNull() then
            None
          else
            Some(v)
        val b = List.newBuilder[StageRunRecord]
        while rs.next() do
          b +=
            StageRunRecord(
              name = rs.getString(1),
              state = rs.getString(2),
              attempts = rs.getInt(3),
              error = Option(rs.getString(4)),
              table = Option(rs.getString(5)),
              waitingSinceMillis = getLongOpt(6),
              lastPollAtMillis = getLongOpt(7)
            )
        b.result()
      }
    }

end PostgresFlowRunStore

object PostgresFlowRunStore:
  /** JDBC URL of the shared run-store database, e.g. jdbc:postgresql://host:5432/wvlet */
  val URL_ENV = "WVLET_FLOW_STORE_PG_URL"

  /** User of the run-store database (default: postgres) */
  val USER_ENV = "WVLET_FLOW_STORE_PG_USER"

  /** Password of the run-store database (default: empty) */
  val PASSWORD_ENV = "WVLET_FLOW_STORE_PG_PASSWORD"

  /**
    * Create a store from the WVLET_FLOW_STORE_PG_* environment variables. The connection settings
    * come from the environment only — never from CLI flags — so that credentials stay out of the
    * process list
    */
  def fromEnv(): PostgresFlowRunStore =
    val url = sys
      .env
      .getOrElse(
        URL_ENV,
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"The postgres run store requires the ${URL_ENV} environment variable (e.g. jdbc:postgresql://host:5432/wvlet)"
          )
      )
    PostgresFlowRunStore(
      url,
      sys.env.getOrElse(USER_ENV, "postgres"),
      sys.env.getOrElse(PASSWORD_ENV, "")
    )

end PostgresFlowRunStore

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

import wvlet.uni.log.LogSupport

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import scala.util.Using
import scala.util.control.NonFatal

/**
  * A SQLite-backed flow run store (`<targetFolder>/flow-runs/registry.db`). Unlike the JSON file
  * store, records are transactional across processes: WAL mode supports a writer concurrent with
  * readers (e.g. a scheduler daemon plus CLI commands), and run-slot claims for `concurrency:`
  * enforcement are atomic single-statement inserts.
  *
  * The store keeps one connection per instance; all operations are synchronized on it
  */
class SQLiteFlowRunStore(dbPath: Path) extends FlowRunStore with LogSupport:
  Option(dbPath.getParent).foreach(Files.createDirectories(_))

  private val conn: Connection = DriverManager.getConnection(s"jdbc:sqlite:${dbPath}")

  Using.resource(conn.createStatement()) { stmt =>
    stmt.execute("pragma journal_mode = WAL")
    stmt.execute("pragma busy_timeout = 10000")
    stmt.execute("""create table if not exists runs(
        |  run_id           text primary key,
        |  flow_name        text not null,
        |  state            text not null,
        |  started_at       integer not null,
        |  finished_at      integer,
        |  cancel_requested integer not null default 0
        |)""".stripMargin)
    stmt.execute("""create table if not exists stages(
        |  run_id     text not null,
        |  ordinal    integer not null,
        |  name       text not null,
        |  state      text not null,
        |  attempts   integer not null,
        |  error      text,
        |  table_name text,
        |  primary key(run_id, ordinal)
        |)""".stripMargin)
  }

  override def save(record: FlowRunRecord): Unit = synchronized {
    conn.setAutoCommit(false)
    try
      Using.resource(
        conn.prepareStatement("""insert into runs(run_id, flow_name, state, started_at, finished_at)
            |values(?, ?, ?, ?, ?)
            |on conflict(run_id) do update set
            |  flow_name = excluded.flow_name,
            |  state = excluded.state,
            |  started_at = excluded.started_at,
            |  finished_at = excluded.finished_at""".stripMargin)
      ) { ps =>
        ps.setString(1, record.runId.toLowerCase)
        ps.setString(2, record.flowName)
        ps.setString(3, record.state)
        ps.setLong(4, record.startedAtMillis)
        record.finishedAtMillis match
          case Some(f) =>
            ps.setLong(5, f)
          case None =>
            ps.setNull(5, java.sql.Types.BIGINT)
        ps.executeUpdate()
      }
      saveStages(record)
      conn.commit()
    catch
      case NonFatal(e) =>
        conn.rollback()
        throw e
    finally
      conn.setAutoCommit(true)
  }

  private def saveStages(record: FlowRunRecord): Unit =
    Using.resource(conn.prepareStatement("delete from stages where run_id = ?")) { ps =>
      ps.setString(1, record.runId.toLowerCase)
      ps.executeUpdate()
    }
    Using.resource(
      conn.prepareStatement(
        "insert into stages(run_id, ordinal, name, state, attempts, error, table_name) values(?, ?, ?, ?, ?, ?, ?)"
      )
    ) { ps =>
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
          ps.addBatch()
        }
      ps.executeBatch()
    }

  override def get(runId: String): Option[FlowRunRecord] = synchronized {
    queryRuns("where run_id = ?", _.setString(1, runId.toLowerCase)).headOption
  }

  override def list(): List[FlowRunRecord] = synchronized {
    queryRuns("order by started_at desc, run_id desc", _ => ())
  }

  override def claimRunSlot(record: FlowRunRecord, concurrencyLimit: Int): Boolean = synchronized {
    // A single insert statement is atomic in SQLite, so the running-count check and the claim
    // cannot interleave with claims from other processes
    val claimed =
      Using.resource(
        conn.prepareStatement("""insert into runs(run_id, flow_name, state, started_at, finished_at)
          |select ?, ?, ?, ?, ?
          |where (select count(*) from runs where flow_name = ? and state = ?) < ?""".stripMargin)
      ) { ps =>
        ps.setString(1, record.runId.toLowerCase)
        ps.setString(2, record.flowName)
        ps.setString(3, record.state)
        ps.setLong(4, record.startedAtMillis)
        record.finishedAtMillis match
          case Some(f) =>
            ps.setLong(5, f)
          case None =>
            ps.setNull(5, java.sql.Types.BIGINT)
        ps.setString(6, record.flowName)
        ps.setString(7, FlowRunRecord.STATE_RUNNING)
        ps.setInt(8, concurrencyLimit)
        ps.executeUpdate() == 1
      }
    if claimed then
      saveStages(record)
    claimed
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
          s"select run_id, flow_name, state, started_at, finished_at from runs ${clause}"
        )
      ) { ps =>
        bind(ps)
        Using.resource(ps.executeQuery()) { rs =>
          val b = List.newBuilder[FlowRunRecord]
          while rs.next() do
            val finishedAt = rs.getLong(5)
            // wasNull refers to the immediately preceding getLong(5) read
            val finishedAtOpt =
              if rs.wasNull() then
                None
              else
                Some(finishedAt)
            b +=
              FlowRunRecord(
                runId = rs.getString(1),
                flowName = rs.getString(2),
                state = rs.getString(3),
                startedAtMillis = rs.getLong(4),
                finishedAtMillis = finishedAtOpt
              )
          b.result()
        }
      }
    runs.map(r => r.copy(stages = stagesOf(r.runId)))

  end queryRuns

  private def stagesOf(runId: String): List[StageRunRecord] =
    Using.resource(
      conn.prepareStatement(
        "select name, state, attempts, error, table_name from stages where run_id = ? order by ordinal"
      )
    ) { ps =>
      ps.setString(1, runId.toLowerCase)
      Using.resource(ps.executeQuery()) { rs =>
        val b = List.newBuilder[StageRunRecord]
        while rs.next() do
          b +=
            StageRunRecord(
              name = rs.getString(1),
              state = rs.getString(2),
              attempts = rs.getInt(3),
              error = Option(rs.getString(4)),
              table = Option(rs.getString(5))
            )
        b.result()
      }
    }

end SQLiteFlowRunStore

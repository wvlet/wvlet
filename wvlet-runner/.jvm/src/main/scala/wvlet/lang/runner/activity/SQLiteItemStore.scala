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
package wvlet.lang.runner.activity

import wvlet.uni.log.LogSupport

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.util.Properties
import scala.util.Using
import scala.util.control.NonFatal

/**
  * A SQLite-backed item store for fan-out batch stages. Shares the same database file as
  * [[wvlet.lang.runner.SQLiteFlowRunStore]] when co-located, adding an `items` table alongside the
  * existing `runs` and `stages` tables.
  *
  * All operations are synchronized on the connection for thread safety
  */
class SQLiteItemStore(dbPath: Path) extends ItemStore with LogSupport:
  Option(dbPath.getParent).foreach(Files.createDirectories(_))

  // Connect through the driver itself: DriverManager only returns drivers visible to the caller's
  // class loader, which fails when the driver was registered by another loader (e.g. a server
  // embedded in a test JVM)
  private val conn: Connection = org.sqlite.JDBC().connect(s"jdbc:sqlite:${dbPath}", Properties())

  Using.resource(conn.createStatement()) { stmt =>
    stmt.execute("pragma journal_mode = WAL")
    stmt.execute("pragma busy_timeout = 10000")
    stmt.execute("""create table if not exists items(
        |  item_id          text primary key,
        |  run_id           text not null,
        |  stage_name       text not null,
        |  item_key         text not null,
        |  state            text not null,
        |  attempt_id       text not null,
        |  attempts         integer not null default 0,
        |  output           text,
        |  error            text,
        |  started_at       integer,
        |  finished_at      integer,
        |  lease_expires_at integer,
        |  activity_type    text,
        |  input            text,
        |  timeout_ms       integer,
        |  heartbeat_ms     integer,
        |  worker_id        text
        |)""".stripMargin)
    // Databases created before task-queue dispatch lack the dispatch columns; add them in place
    val existing =
      Using.resource(stmt.executeQuery("pragma table_info(items)")) { rs =>
        val b = Set.newBuilder[String]
        while rs.next() do
          b += rs.getString("name")
        b.result()
      }
    SQLiteItemStore
      .dispatchColumns
      .foreach { (name, tpe) =>
        if !existing.contains(name) then
          stmt.execute(s"alter table items add column ${name} ${tpe}")
      }
    stmt.execute("create index if not exists idx_items_run_stage on items(run_id, stage_name)")
    stmt.execute("create index if not exists idx_items_state on items(state)")
  }

  override def saveItem(record: ItemRecord): Unit = synchronized {
    Using.resource(
      conn.prepareStatement(
        """insert into items(item_id, run_id, stage_name, item_key, state, attempt_id, attempts,
          |  output, error, started_at, finished_at, lease_expires_at, activity_type, input,
          |  timeout_ms, heartbeat_ms, worker_id)
          |values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |on conflict(item_id) do update set
          |  state = excluded.state,
          |  attempt_id = excluded.attempt_id,
          |  attempts = excluded.attempts,
          |  output = excluded.output,
          |  error = excluded.error,
          |  started_at = excluded.started_at,
          |  finished_at = excluded.finished_at,
          |  lease_expires_at = excluded.lease_expires_at,
          |  activity_type = excluded.activity_type,
          |  input = excluded.input,
          |  timeout_ms = excluded.timeout_ms,
          |  heartbeat_ms = excluded.heartbeat_ms,
          |  worker_id = excluded.worker_id""".stripMargin
      )
    ) { ps =>
      ps.setString(1, record.itemId)
      ps.setString(2, record.runId)
      ps.setString(3, record.stageName)
      ps.setString(4, record.itemKey)
      ps.setString(5, record.state)
      ps.setString(6, record.attemptId)
      ps.setInt(7, record.attempts)
      ps.setString(8, record.output.orNull)
      ps.setString(9, record.error.orNull)
      setLongOpt(ps, 10, record.startedAtMillis)
      setLongOpt(ps, 11, record.finishedAtMillis)
      setLongOpt(ps, 12, record.leaseExpiresAtMillis)
      ps.setString(13, record.activityType.orNull)
      ps.setString(14, record.input.orNull)
      setLongOpt(ps, 15, record.timeoutMillis)
      setLongOpt(ps, 16, record.heartbeatMillis)
      ps.setString(17, record.workerId.orNull)
      ps.executeUpdate()
    }
  }

  override def getItem(itemId: String): Option[ItemRecord] = synchronized {
    Using.resource(conn.prepareStatement("select * from items where item_id = ?")) { ps =>
      ps.setString(1, itemId)
      Using.resource(ps.executeQuery())(rs =>
        if rs.next() then
          Some(readItem(rs))
        else
          None
      )
    }
  }

  override def listItems(runId: String, stageName: String): List[ItemRecord] = synchronized {
    Using.resource(
      conn.prepareStatement(
        "select * from items where run_id = ? and stage_name = ? order by rowid"
      )
    ) { ps =>
      ps.setString(1, runId)
      ps.setString(2, stageName)
      readItems(ps)
    }
  }

  override def listItemsPage(
      runId: String,
      stageName: String,
      state: Option[String],
      offset: Int,
      limit: Int
  ): List[ItemRecord] = synchronized {
    Using.resource(conn.prepareStatement("""select * from items
          |where run_id = ? and stage_name = ? and (? is null or state = ?)
          |order by rowid limit ? offset ?""".stripMargin)) { ps =>
      ps.setString(1, runId)
      ps.setString(2, stageName)
      ps.setString(3, state.orNull)
      ps.setString(4, state.orNull)
      ps.setInt(5, limit)
      ps.setInt(6, offset)
      readItems(ps)
    }
  }

  override def countItemsByState(runId: String, stageName: String): Map[String, Int] =
    synchronized {
      Using.resource(
        conn.prepareStatement(
          "select state, count(*) as cnt from items where run_id = ? and stage_name = ? group by state"
        )
      ) { ps =>
        ps.setString(1, runId)
        ps.setString(2, stageName)
        Using.resource(ps.executeQuery()) { rs =>
          val b = Map.newBuilder[String, Int]
          while rs.next() do
            b += rs.getString("state") -> rs.getInt("cnt")
          b.result()
        }
      }
    }

  override def listActiveItems(): List[ItemRecord] = synchronized {
    Using.resource(
      conn.prepareStatement(
        s"select * from items where state in (${SQLiteItemStore.activeStates}) order by rowid"
      )
    )(readItems)
  }

  override def claimItem(
      itemId: String,
      attemptId: String,
      workerId: String,
      startedAtMillis: Long,
      leaseExpiresAtMillis: Long
  ): Boolean = synchronized {
    Using.resource(
      conn.prepareStatement("""update items set state = 'running', worker_id = ?, started_at = ?,
          |  lease_expires_at = ?
          |where item_id = ? and attempt_id = ? and state = 'queued'""".stripMargin)
    ) { ps =>
      ps.setString(1, workerId)
      ps.setLong(2, startedAtMillis)
      ps.setLong(3, leaseExpiresAtMillis)
      ps.setString(4, itemId)
      ps.setString(5, attemptId)
      ps.executeUpdate() == 1
    }
  }

  override def markCancelling(itemId: String, attemptId: String): Boolean = synchronized {
    Using.resource(
      conn.prepareStatement("""update items set state = 'cancelling'
          |where item_id = ? and attempt_id = ? and state in ('queued', 'running')""".stripMargin)
    ) { ps =>
      ps.setString(1, itemId)
      ps.setString(2, attemptId)
      ps.executeUpdate() == 1
    }
  }

  override def reportResult(
      itemId: String,
      attemptId: String,
      state: String,
      output: Option[String],
      error: Option[String],
      finishedAtMillis: Long
  ): Boolean = synchronized {
    Using.resource(
      conn.prepareStatement(s"""update items set state = ?, output = ?, error = ?, finished_at = ?,
          |  lease_expires_at = null
          |where item_id = ? and attempt_id = ?
          |  and state not in (${SQLiteItemStore.finalStates})""".stripMargin)
    ) { ps =>
      ps.setString(1, state)
      ps.setString(2, output.orNull)
      ps.setString(3, error.orNull)
      ps.setLong(4, finishedAtMillis)
      ps.setString(5, itemId)
      ps.setString(6, attemptId)
      ps.executeUpdate() == 1
    }
  }

  override def refreshItemLease(
      itemId: String,
      attemptId: String,
      leaseExpiresAtMillis: Long
  ): Unit = synchronized {
    Using.resource(
      conn.prepareStatement(
        "update items set lease_expires_at = ? where item_id = ? and attempt_id = ?"
      )
    ) { ps =>
      ps.setLong(1, leaseExpiresAtMillis)
      ps.setString(2, itemId)
      ps.setString(3, attemptId)
      ps.executeUpdate()
    }
  }

  override def deleteItems(runId: String): Unit = synchronized {
    Using.resource(conn.prepareStatement("delete from items where run_id = ?")) { ps =>
      ps.setString(1, runId)
      ps.executeUpdate()
    }
  }

  override def close(): Unit = synchronized {
    conn.close()
  }

  private def setLongOpt(ps: java.sql.PreparedStatement, index: Int, value: Option[Long]): Unit =
    value match
      case Some(v) =>
        ps.setLong(index, v)
      case None =>
        ps.setNull(index, java.sql.Types.BIGINT)

  private def readItems(ps: java.sql.PreparedStatement): List[ItemRecord] =
    Using.resource(ps.executeQuery()) { rs =>
      val b = List.newBuilder[ItemRecord]
      while rs.next() do
        b += readItem(rs)
      b.result()
    }

  private def readItem(rs: java.sql.ResultSet): ItemRecord =
    def getLongOpt(column: String): Option[Long] =
      val v = rs.getLong(column)
      if rs.wasNull() then
        None
      else
        Some(v)
    ItemRecord(
      itemId = rs.getString("item_id"),
      runId = rs.getString("run_id"),
      stageName = rs.getString("stage_name"),
      itemKey = rs.getString("item_key"),
      state = rs.getString("state"),
      attemptId = rs.getString("attempt_id"),
      attempts = rs.getInt("attempts"),
      output = Option(rs.getString("output")),
      error = Option(rs.getString("error")),
      startedAtMillis = getLongOpt("started_at"),
      finishedAtMillis = getLongOpt("finished_at"),
      leaseExpiresAtMillis = getLongOpt("lease_expires_at"),
      activityType = Option(rs.getString("activity_type")),
      input = Option(rs.getString("input")),
      timeoutMillis = getLongOpt("timeout_ms"),
      heartbeatMillis = getLongOpt("heartbeat_ms"),
      workerId = Option(rs.getString("worker_id"))
    )

end SQLiteItemStore

object SQLiteItemStore:
  private def sqlList(states: Iterable[ItemState]): String = states
    .map(s => s"'${s.stateName}'")
    .mkString(", ")

  /** Non-terminal states, matching [[ItemState.isTerminal]] */
  private val activeStates: String = sqlList(ItemState.values.filterNot(_.isTerminal))

  /** States a result report must not overwrite, matching [[ItemState.isFinal]] */
  private val finalStates: String = sqlList(ItemState.values.filter(_.isFinal))

  /** Columns added for task-queue dispatch, migrated into databases created without them */
  private val dispatchColumns: List[(String, String)] = List(
    "activity_type" -> "text",
    "input"         -> "text",
    "timeout_ms"    -> "integer",
    "heartbeat_ms"  -> "integer",
    "worker_id"     -> "text"
  )

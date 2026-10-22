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
import java.sql.DriverManager
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

  private val conn: Connection = DriverManager.getConnection(s"jdbc:sqlite:${dbPath}")

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
        |  lease_expires_at integer
        |)""".stripMargin)
    stmt.execute("create index if not exists idx_items_run_stage on items(run_id, stage_name)")
  }

  override def saveItem(record: ItemRecord): Unit = synchronized {
    Using.resource(
      conn.prepareStatement(
        """insert into items(item_id, run_id, stage_name, item_key, state, attempt_id, attempts,
          |  output, error, started_at, finished_at, lease_expires_at)
          |values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          |on conflict(item_id) do update set
          |  state = excluded.state,
          |  attempt_id = excluded.attempt_id,
          |  attempts = excluded.attempts,
          |  output = excluded.output,
          |  error = excluded.error,
          |  started_at = excluded.started_at,
          |  finished_at = excluded.finished_at,
          |  lease_expires_at = excluded.lease_expires_at""".stripMargin
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
      Using.resource(ps.executeQuery()) { rs =>
        val b = List.newBuilder[ItemRecord]
        while rs.next() do
          b += readItem(rs)
        b.result()
      }
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
      conn.prepareStatement("""update items set state = ?, output = ?, error = ?, finished_at = ?,
          |  lease_expires_at = null
          |where item_id = ? and attempt_id = ?""".stripMargin)
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
      leaseExpiresAtMillis = getLongOpt("lease_expires_at")
    )

end SQLiteItemStore

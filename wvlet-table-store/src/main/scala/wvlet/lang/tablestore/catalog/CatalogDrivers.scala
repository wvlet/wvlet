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
package wvlet.lang.tablestore.catalog

import java.util.Properties

/**
  * Catalog driver factory. All three backends run the same portable SQL through
  * [[JdbcCatalogStore]]; only connectivity differs.
  */
object CatalogDrivers:

  /** JDBC 4 auto-discovery does not always survive test classloaders; load drivers explicitly */
  private def loadDriver(className: String): Unit =
    try
      Class.forName(className)
    catch
      case e: ClassNotFoundException =>
        throw wvlet.lang.tablestore.TableStoreException(s"JDBC driver not found: ${className}", e)

  /** SQLite catalog — the dev/test backend of the embedded profile */
  def sqlite(path: String): JdbcCatalogStore =
    loadDriver("org.sqlite.JDBC")
    val props = new Properties()
    props.setProperty("busy_timeout", "10000")
    props.setProperty("journal_mode", "WAL")
    new JdbcCatalogStore(s"jdbc:sqlite:${path}", user = None, password = None)

  def inMemorySqlite(): JdbcCatalogStore = sqlite(":memory:")

  /** DuckDB catalog — the embedded profile backend sharing the engine with the reader */
  def duckdb(path: String): JdbcCatalogStore =
    loadDriver("org.duckdb.DuckDBDriver")
    new JdbcCatalogStore(
      if path.isEmpty || path == ":memory:" then
        "jdbc:duckdb:"
      else
        s"jdbc:duckdb:${path}"
    )

  def inMemoryDuckDB(): JdbcCatalogStore = duckdb(":memory:")

  /** Postgres catalog — the production single-tenant / multi-tenant service backend */
  def postgres(
      host: String,
      port: Int,
      database: String,
      user: String,
      password: String
  ): JdbcCatalogStore =
    loadDriver("org.postgresql.Driver")
    new JdbcCatalogStore(
      s"jdbc:postgresql://${host}:${port}/${database}",
      user = Some(user),
      password = Some(password)
    )

end CatalogDrivers

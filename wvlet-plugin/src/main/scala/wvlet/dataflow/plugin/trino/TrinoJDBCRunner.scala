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
package wvlet.dataflow.plugin.trino

import io.trino.jdbc.{TrinoConnection, TrinoDriver}
import wvlet.airframe.control.Control
import wvlet.log.LogSupport

import java.sql.DriverManager
import java.util.Properties

class TrinoJDBCRunner(driver: TrinoJDBCDriver):

  def withConnection[U](service: TrinoService, schema: String)(body: TrinoConnection => U): U =
    Control.withResource(
      driver.newConnection(service.address, service.connector, schema, service.user)
    ) { conn =>
      body(conn)
    }

class TrinoJDBCDriver extends AutoCloseable with LogSupport:

  private val driver: TrinoDriver =
    info("Initializing TrinoDriver")
    Class.forName("io.trino.jdbc.TrinoDriver")
    // Need to cast to TrinoDriver to set session properties
    DriverManager.getDriver(s"jdbc:trino://").asInstanceOf[TrinoDriver]

  def newConnection(
      hostname: String,
      catalog: String,
      schema: String,
      user: String
  ): TrinoConnection =
    val p = new Properties()
    p.setProperty("user", user)
    val connectAddress = s"jdbc:trino://${hostname}/${catalog}/${schema}"
    val conn           = driver.connect(connectAddress, p).asInstanceOf[TrinoConnection]
    conn

  override def close(): Unit =
    info(s"Closing TrinoJDBCDriver")
    driver.close()

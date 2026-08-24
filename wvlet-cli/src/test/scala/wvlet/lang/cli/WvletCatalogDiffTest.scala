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
package wvlet.lang.cli

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.SourceIO

import java.nio.file.Files
import java.nio.file.Path

class WvletCatalogDiffTest extends UniTest:

  test("help") {
    WvletMain.main("catalog diff --help")
  }

  test("exit successfully when no declared table exists in the catalog yet") {
    Files.createDirectories(Path.of("target"))
    val projectDir = Files.createTempDirectory(Path.of("target"), "catalog-diff").toString
    SourceIO.writeString(
      s"${projectDir}/tables.wv",
      """table users = {
        |  id: int
        |  name: string
        |}
        |""".stripMargin
    )
    // The default profile connects to a fresh in-memory DuckDB: the declared table is not
    // created yet, which is not drift (it materializes on the first write)
    WvletMain.main(s"catalog diff -w ${projectDir}")
  }

end WvletCatalogDiffTest

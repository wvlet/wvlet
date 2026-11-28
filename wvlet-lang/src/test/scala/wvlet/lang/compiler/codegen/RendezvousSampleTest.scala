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
package wvlet.lang.compiler.codegen

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.*
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.model.plan.RendezvousSampling
import wvlet.uni.test.UniTest

/** `sample rendezvous(p%) by keys`: parsing, printing, and SQL generation */
class RendezvousSampleTest extends UniTest:

  private val query =
    """from [[1, 'a'], [2, 'b']] as rv(id, name)
      |sample rendezvous(10%) by id""".stripMargin

  private def generateSQL(wv: String, dbType: DBType = DBType.DuckDB): String =
    val compiler = Compiler(CompilerOptions(workEnv = WorkEnv("."), dbType = dbType))
    val unit     = CompilationUnit.fromWvletString(wv)
    val result   = compiler.compileSingleUnit(unit)
    GenSQL.generateSQL(unit)(using result.context)

  private def print(wv: String): String = WvletGenerator().print(
    ParserPhase.parseOnly(CompilationUnit.fromWvletString(wv))
  )

  test("should generate a hash filter for DuckDB") {
    val sql = generateSQL(query)
    debug(sql)
    sql shouldContain "id is not null"
    sql shouldContain s"(hash(0, id) >> 11) < ${RendezvousSampling.threshold(10)}"
  }

  test("should hash the seed and every key") {
    val sql = generateSQL("""from [[1, 'a'], [2, 'b']] as rv(id, name)
        |sample rendezvous(25%, seed = -3) by id, name""".stripMargin)
    sql shouldContain "hash(-3, id, name)"
    sql shouldContain "name is not null"
  }

  test("should generate xxhash64 for Trino") {
    val sql = generateSQL(query, DBType.Trino)
    debug(sql)
    sql shouldContain "bitwise_right_shift("
    sql shouldContain "from_big_endian_64("
    sql shouldContain
      "xxhash64(to_utf8(concat_ws(chr(1), cast(0 as varchar), cast(id as varchar))))"
    sql shouldContain s"< ${RendezvousSampling.threshold(10)}"
  }

  test("should keep no rows at 0% and every non-null key at 100%") {
    generateSQL(query.replace("10%", "0%")) shouldContain "where false"
    val all = generateSQL(query.replace("10%", "100%"))
    all shouldContain "id is not null"
    all shouldNotContain "hash("
  }

  test("should reject databases without a supported hash") {
    val e = intercept[WvletLangException] {
      generateSQL(query, DBType.MySQL)
    }
    e.statusCode shouldBe StatusCode.NOT_IMPLEMENTED
  }

  test("should print the sample back as Wvlet") {
    val printed = print("""from t
        |sample rendezvous(10%, seed = 42) by user_id, day""".stripMargin)
    printed shouldContain "sample rendezvous(10.0%, seed = 42) by user_id, day"
    print(printed) shouldContain "sample rendezvous(10.0%, seed = 42) by user_id, day"
    print("from t\nsample rendezvous(5%) by user_id") shouldContain
      "sample rendezvous(5.0%) by user_id"
  }

  test("should require a percentage and sampling keys") {
    val rows = intercept[WvletLangException] {
      print("from t\nsample rendezvous(10) by user_id")
    }
    rows.statusCode shouldBe StatusCode.SYNTAX_ERROR
    rows.getMessage shouldContain "takes a percentage"
    intercept[WvletLangException] {
      print("from t\nsample rendezvous(10%)")
    }.statusCode shouldBe StatusCode.SYNTAX_ERROR
  }

  test("should reject a seed that is not a long") {
    intercept[WvletLangException] {
      print("from t\nsample rendezvous(10%, seed = 99999999999999999999) by user_id")
    }.statusCode shouldBe StatusCode.SYNTAX_ERROR
  }

  test("should compute thresholds of the top hash bits") {
    RendezvousSampling.threshold(0) shouldBe 0L
    RendezvousSampling.threshold(-1) shouldBe 0L
    RendezvousSampling.threshold(100) shouldBe (1L << RendezvousSampling.HashBits)
    RendezvousSampling.threshold(50) shouldBe (1L << (RendezvousSampling.HashBits - 1))
  }

end RendezvousSampleTest

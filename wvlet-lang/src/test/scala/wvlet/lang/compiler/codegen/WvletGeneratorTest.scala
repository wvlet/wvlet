package wvlet.lang.compiler.codegen

import wvlet.uni.test.UniTest
import wvlet.lang.compiler.parser.ParserPhase
import wvlet.lang.compiler.CompilationUnit

class WvletGeneratorTest extends UniTest:

  private def print(wv: String): String =
    val unit = CompilationUnit.fromWvletString(wv)
    val plan = ParserPhase.parseOnly(unit)
    WvletGenerator().print(plan)

  test("should preserve names of named function arguments") {
    val printed = print("""from t
        |select approx_percentile(price, percentile = 0.95)""".stripMargin)
    printed shouldContain "percentile = 0.95"

    // The printed query should parse back to the same named argument
    val reprinted = print(printed)
    reprinted shouldContain "percentile = 0.95"
  }

  test("should print positional and named arguments together") {
    val printed = print("""from t
        |select f(a, b, mode = 'fast')""".stripMargin)
    printed shouldContain "f(a, b, mode = 'fast')"
  }

  test("should keep distinct arguments without names") {
    val printed = print("""from t
        |select count(distinct user_id)""".stripMargin)
    printed shouldContain "count(distinct user_id)"
  }

  test("should print run flow arguments") {
    val printed = print("run flow F('a', min_id = 10)")
    printed shouldContain "run flow F('a', min_id = 10)"

    // The printed statement should parse back to the same flow call
    print(printed) shouldContain "run flow F('a', min_id = 10)"
  }

  test("should print a run flow statement without arguments as a bare name") {
    val printed = print("run flow F")
    printed shouldContain "run flow F"
    printed.contains("F()") shouldBe false
  }

  test("should print call statement arguments in name: value form") {
    val printed = print("call slack.post_message(channel: '#reports', text: 'hello')")
    printed shouldContain "call slack.post_message(channel: '#reports', text: 'hello')"

    // The printed statement should parse back to the same tool call
    print(printed) shouldContain "call slack.post_message(channel: '#reports', text: 'hello')"
  }

  test("should round-trip schema statements with trailing modifiers") {
    val printed = print("""create schema staging if not exists
        |drop schema staging if exists""".stripMargin)
    printed shouldContain "create schema staging if not exists"
    printed shouldContain "drop schema staging if exists"
    print(printed) shouldContain "create schema staging if not exists"
  }

  test("should round-trip a table shape declaration as `table`, not `type`") {
    val printed = print("""table users = {
        |  id: int
        |  name: string
        |}""".stripMargin)
    printed shouldContain "table users"
    printed shouldContain "id: int"
    printed.contains("type users") shouldBe false
    print(printed) shouldContain "table users"
  }

  test("should round-trip table field defaults (#1997)") {
    val printed = print("""table users = {
        |  id: int
        |  active: boolean = true
        |  created_at: timestamp = now()
        |}""".stripMargin)
    printed shouldContain "active: boolean = true"
    printed shouldContain "= now()"
    print(printed) shouldContain "active: boolean = true"
  }

  test("should round-trip a model with a type annotation") {
    val printed = print("""model rm_all: rm_users = {
        |  from rm_users
        |}""".stripMargin)
    printed shouldContain "model rm_all: rm_users"
    print(printed) shouldContain "model rm_all: rm_users"
  }

  test("should round-trip schema locations, schema options, and cascade drops") {
    val printed = print(
      """create schema sales in 's3://bucket/sales/' if not exists with owner: 'etl'
        |drop schema staging if exists with cascade: true""".stripMargin
    )
    printed shouldContain
      "create schema sales in 's3://bucket/sales/' if not exists with owner: 'etl'"
    printed shouldContain "drop schema staging if exists with cascade: true"
    print(printed) shouldContain "drop schema staging if exists with cascade: true"
  }

  test("should round-trip a like-based table declaration") {
    val printed = print("""table users = {
        |  id: int
        |}
        |table users_backup like users""".stripMargin)
    printed shouldContain "table users_backup like users"
    print(printed) shouldContain "table users_backup like users"
  }

  test("should round-trip a trait declaration as `trait`, not `type`") {
    val printed = print("""trait ip_address in duckdb extends string = {
        |  def country_name: string = sql"'N/A'"
        |}""".stripMargin)
    printed shouldContain "trait ip_address in duckdb extends string"
    printed.contains("type ip_address") shouldBe false
    print(printed) shouldContain "trait ip_address in duckdb extends string"
  }

  test("should round-trip comma-separated mixin parent lists (#2012)") {
    val printed = print("""type timestamped = {
        |  created_at: string
        |}
        |trait auditable = {
        |  def tag: string = sql"'a'"
        |}
        |table events extends timestamped, auditable = {
        |  id: int
        |}
        |trait combined extends auditable, string = {
        |  def tag2: string = sql"'b'"
        |}""".stripMargin)
    printed shouldContain "table events extends timestamped, auditable"
    printed shouldContain "trait combined extends auditable, string"
    print(printed) shouldContain "table events extends timestamped, auditable"
    print(printed) shouldContain "trait combined extends auditable, string"
  }

  test("should round-trip a table declaration with an `in` binding") {
    val printed = print("""table events in mydb.analytics = {
        |  id: int
        |  label: string
        |}""".stripMargin)
    printed shouldContain "table events in mydb.analytics"
    print(printed) shouldContain "table events in mydb.analytics"
  }

  test("should round-trip table actions") {
    val printed = print("""create table users
        |truncate users
        |drop table users if exists""".stripMargin)
    printed shouldContain "create table users"
    printed shouldContain "truncate users"
    printed shouldContain "drop table users if exists"
    print(printed) shouldContain "drop table users if exists"
  }

  test("should round-trip save to with the if not exists modifier") {
    val printed = print("""from t
        |save to snapshot if not exists""".stripMargin)
    printed shouldContain "save to snapshot if not exists"
    print(printed) shouldContain "save to snapshot if not exists"
  }

  test("should round-trip a reshape block") {
    val printed = print("""reshape users {
        |  add email: string
        |  rename name as full_name
        |  exclude age
        |  cast age as long
        |}""".stripMargin)
    printed shouldContain "reshape users"
    printed shouldContain "add email: string"
    printed shouldContain "rename name as full_name"
    printed shouldContain "exclude age"
    printed shouldContain "cast age as long"
    print(printed) shouldContain "cast age as long"
  }

  test("should round-trip rename statements") {
    val printed = print("""rename table users to customers
        |rename schema staging to archive""".stripMargin)
    printed shouldContain "rename table users to customers"
    printed shouldContain "rename schema staging to archive"
    print(printed) shouldContain "rename table users to customers"
  }

  test("should round-trip save as view in flow and block forms") {
    val printed = print("""from t
        |save as view active_users""".stripMargin)
    printed shouldContain "save as view active_users"
    print(printed) shouldContain "save as view active_users"

    val block = print("""save as view v2 {
        |  from t
        |}""".stripMargin)
    block shouldContain "save as view v2"
  }

  test("should round-trip drop view") {
    val printed = print("drop view v1 if exists")
    printed shouldContain "drop view v1 if exists"
    print(printed) shouldContain "drop view v1 if exists"
  }

  test("should round-trip a flow update") {
    val printed = print("""from users
        |where id = 2
        |update status = 'dormant', name = 'x'""".stripMargin)
    printed shouldContain "update status = 'dormant', name = 'x'"
    print(printed) shouldContain "update status = 'dormant', name = 'x'"
  }

  test("should round-trip keyed append") {
    val printed = print("""from staged
        |append to users on id, name""".stripMargin)
    printed shouldContain "append to users on id, name"
    print(printed) shouldContain "append to users on id, name"
  }

  test("should round-trip SQL-equivalent create table forms") {
    val printed = print("""create table users if not exists
        |create or replace table users""".stripMargin)
    printed shouldContain "create table users if not exists"
    printed shouldContain "create or replace table users"
    print(printed) shouldContain "create or replace table users"
  }

  test("should round-trip a database attachment") {
    val printed = print("use 'archive.duckdb' as archive")
    printed shouldContain "use 'archive.duckdb' as archive"
    print(printed) shouldContain "use 'archive.duckdb' as archive"
  }

  test("should round-trip a database attachment with options") {
    val printed = print("use 'postgres://host/db' as pg with read_only: true")
    printed shouldContain "use 'postgres://host/db' as pg with read_only: true"
    print(printed) shouldContain "use 'postgres://host/db' as pg with read_only: true"
  }

  test("should keep plain use statements as schema/connector switches") {
    val printed = print("use memory.main")
    printed shouldContain "use memory.main"
    printed.contains("attach") shouldBe false
  }

  test("should parse block-form save and append statements") {
    val printed = print("""save to snapshot {
        |  from t
        |}""".stripMargin)
    printed shouldContain "save to snapshot"

    val appended = print("""append to users(id, name) {
        |  from t
        |}""".stripMargin)
    appended shouldContain "append to users(id, name)"
  }

end WvletGeneratorTest

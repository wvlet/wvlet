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
package wvlet.lang.compiler.transform

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.SqlGenerator
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONLong
import wvlet.uni.json.JSON.JSONNull
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import java.sql.DriverManager
import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of the expression rewrite rules (formal/, exported by
  * `lake exe rewrite-expr-traces`) against the shipped [[RewriteExpr]] rules. The model proves the
  * rules keep the value of every expression on every engine, NULL inputs included, and that the
  * phase is idempotent; this test checks that the Scala rules rewrite each traced expression to the
  * model's expression (node types included), and that DuckDB computes the model's values for it.
  */
class RewriteExprConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/compiler/transform/rewrite-expr-traces.json")
        )
      )(_.mkString)
    obj(JSON.parse(text))

  private def obj(v: JSONValue): Map[String, JSONValue] =
    v match
      case JSONObject(entries) =>
        entries.toMap
      case other =>
        fail(s"Expected a JSON object: ${other.toJSON}")

  private def arr(v: JSONValue): List[JSONValue] =
    v match
      case JSONArray(elems) =>
        elems.toList
      case other =>
        fail(s"Expected a JSON array: ${other.toJSON}")

  private def str(v: JSONValue): String =
    v match
      case JSONString(s) =>
        s
      case other =>
        fail(s"Expected a JSON string: ${other.toJSON}")

  private val cases: List[Map[String, JSONValue]] = arr(fixture("cases")).map(obj)

  private def dataType(name: String): DataType =
    name match
      case "string" =>
        DataType.StringType
      case "int" =>
        DataType.LongType
      case "bool" =>
        DataType.BooleanType
      case "null" =>
        DataType.NullType
      case _ =>
        DataType.UnknownType

  private def typeName(dt: DataType): String =
    dt match
      case DataType.StringType =>
        "string"
      case DataType.LongType =>
        "int"
      case DataType.BooleanType =>
        "bool"
      case DataType.NullType =>
        "null"
      case _ =>
        "other"

  /**
    * Build the expression a traced node stands for, with the Typer's type on every node
    */
  private def build(v: JSONValue): Expression =
    val m             = obj(v)
    val e: Expression =
      if m.contains("lit") then
        m("lit") match
          case _: JSONNull =>
            NullLiteral(NoSpan)
          case JSONString(s) =>
            StringLiteral.fromString(s, NoSpan)
          case JSONLong(n) =>
            LongLiteral(n, n.toString, NoSpan)
          case JSONBoolean(b) =>
            if b then
              TrueLiteral(NoSpan)
            else
              FalseLiteral(NoSpan)
          case other =>
            fail(s"Unexpected literal: ${other.toJSON}")
      else if m.contains("col") then
        UnquotedIdentifier(str(m("col")), NoSpan)
      else if m.contains("add") then
        val List(l, r) = arr(m("add")).map(build): @unchecked
        ArithmeticBinaryExpr(BinaryExprType.Add, l, r, NoSpan)
      else if m.contains("interp") then
        InterpolatedString(
          NameExpr.fromString(str(m("interp"))),
          arr(m("parts")).map(build),
          DataType.StringType,
          isTripleQuote = false,
          NoSpan
        )
      else if m.contains("part") then
        StringPart(str(m("part")), NoSpan)
      else if m.contains("call") then
        FunctionApply(
          NameExpr.fromString(str(m("call"))),
          arr(m("args")).map(a => FunctionArg(None, build(a), false, Nil, NoSpan)),
          window = None,
          span = NoSpan
        )
      else if m.contains("ifExpr") then
        val List(c, t, f) = arr(m("ifExpr")).map(build): @unchecked
        IfExpr(c, t, f, NoSpan)
      else if m.contains("cast") then
        Cast(build(m("cast")), DataType.StringType, tryCast = false, NoSpan)
      else
        fail(s"Unexpected node: ${v.toJSON}")
    e.tpe = dataType(str(m("type")))
    e

  end build

  /**
    * A canonical text of an expression with the type of every node
    */
  private def shape(e: Expression): String =
    val body =
      e match
        case _: NullLiteral =>
          "null"
        case s: StringLiteral =>
          s"'${s.unquotedValue}'"
        case l: LongLiteral =>
          l.value.toString
        case b: BooleanLiteral =>
          b.booleanValue.toString
        case p: StringPart =>
          s"part(${p.value})"
        case a: ArithmeticBinaryExpr if a.exprType == BinaryExprType.Add =>
          s"(${shape(a.left)} + ${shape(a.right)})"
        case s: InterpolatedString =>
          s"${s.prefix.fullName}\"${s.parts.map(shape).mkString(", ")}\""
        case f: FunctionApply =>
          s"${f.base.asInstanceOf[Identifier].fullName}(${f
              .args
              .map(a => shape(a.value))
              .mkString(", ")})"
        case i: IfExpr =>
          s"ifExpr(${shape(i.cond)}, ${shape(i.onTrue)}, ${shape(i.onFalse)})"
        case c: Cast =>
          s"cast(${shape(c.expr)})"
        case i: Identifier =>
          i.fullName
        case other =>
          fail(s"Unexpected expression: ${other}")
    s"${body}:${typeName(e.dataType)}"

  end shape

  private def rewrite(e: Expression): Expression =
    RewriteExpr
      .rewriteRules
      .foldLeft(e) { (x, rule) =>
        rule.transformExpression(x, Context.NoContext)
      }

  test("rewrite every traced expression to the model's expression") {
    cases.size shouldBe 254
    cases.foreach { c =>
      val input = build(c("input"))
      shape(rewrite(input)) shouldBe shape(build(c("rewritten")))
    }
  }

  test("leave a rewritten expression unchanged when the rules run again") {
    // RewriteExpr runs once per enclosing Query, so nested queries are rewritten repeatedly
    cases.foreach { c =>
      val once = rewrite(build(c("input")))
      shape(rewrite(once)) shouldBe shape(once)
    }
  }

  test("compute the model's values on DuckDB") {
    val rows                                            = arr(fixture("rows")).map(obj)
    def sqlValue(v: JSONValue, sqlType: String): String =
      v match
        case _: JSONNull =>
          s"cast(null as ${sqlType})"
        case JSONString(s) =>
          s"cast('${s}' as ${sqlType})"
        case other =>
          s"cast(${other.toJSON} as ${sqlType})"
    val valuesSql = rows
      .zipWithIndex
      .map { (r, i) =>
        s"(${sqlValue(r("s"), "varchar")}, ${sqlValue(r("n"), "bigint")}, ${sqlValue(
            r("b"),
            "boolean"
          )}, ${i})"
      }
      .mkString(", ")
    val generator = SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.DuckDB))

    Class.forName("org.duckdb.DuckDBDriver")
    Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { conn =>
      var checked = 0
      cases.foreach { c =>
        val expected = arr(c("duckdb"))
        // The model gives an error (e.g. `1 + true`) for some rows; such cases are checked by shape
        val hasError = expected.exists {
          case JSONObject(_) =>
            true
          case _ =>
            false
        }
        if !hasError then
          val sqlExpr = generator.print(rewrite(build(c("input"))))
          val sql     =
            s"select ${sqlExpr} as v from (values ${valuesSql}) as t(s, n, b, id) order by id"
          val actual = List.newBuilder[JSONValue]
          Using.resource(conn.createStatement()) { stmt =>
            Using.resource(stmt.executeQuery(sql)) { rs =>
              while rs.next() do
                actual += (
                  rs.getObject(1) match
                    case null =>
                      JSONNull()
                    case s: String =>
                      JSONString(s)
                    case b: java.lang.Boolean =>
                      JSONBoolean(b)
                    case n: java.lang.Number =>
                      JSONLong(n.longValue())
                    case other =>
                      fail(s"Unexpected value ${other} for ${sql}")
                )
            }
          }
          debug(sql)
          (sql, actual.result()) shouldBe (sql, expected)
          checked += 1
      }
      // Most cases evaluate without errors
      (checked > 200) shouldBe true
    }
  }

  test("match the if function name case-insensitively") {
    val e = FunctionApply(
      NameExpr.fromString("IF"),
      List(
        FunctionArg(None, UnquotedIdentifier("b", NoSpan), false, Nil, NoSpan),
        FunctionArg(None, StringLiteral.fromString("a", NoSpan), false, Nil, NoSpan)
      ),
      window = None,
      span = NoSpan
    )
    e.tpe = DataType.StringType
    rewrite(e) shouldMatch { case IfExpr(_, _, _: NullLiteral, _) =>
    }
  }

end RewriteExprConformanceTest

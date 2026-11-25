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
package wvlet.lang.engine

import wvlet.lang.compiler.lsp.StdLibFunctionDoc
import wvlet.lang.compiler.lsp.StdLibIndex
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

/**
  * Coverage gate for the standard library on the engine. The stdlib's dot-syntax functions are
  * Wvlet's engine-neutral contract (`x.upper`, `x.round(2)`), so this is the primary measure of
  * what users can run on the engine. Every stdlib function visible to the engine's dialect runs
  * once on a one-row sample, on the engine and on DuckDB, and counts as covered when the rows
  * match.
  *
  * `minCovered` pins the current count. Raise it when adding engine support; never lower it. The
  * full report is written to target/engine-stdlib-coverage.txt.
  */
class EngineStdlibCoverageCheck extends UniTest:

  /** Stdlib functions that run on the engine and match DuckDB must not drop below this count */
  private val minCovered = 312

  private val conformance     = EngineConformance(".", Nil, _ => ())
  override def afterAll: Unit = conformance.close()

  private enum Outcome:
    case Covered,
      Mismatch,
      Unsupported,
      EngineError,
      NotProbed

  /**
    * Sample values of a stdlib owner or argument type, as Wvlet expressions. A probe tries the
    * candidates in lockstep (the i-th candidate of every slot, or its last one) until DuckDB
    * accepts the query, so that e.g. `'42'.to_int` and `'2024-03-15'.to_date` both find a valid
    * input
    */
  private def samples(tpe: String, argName: String = ""): Seq[String] =
    tpe.stripSuffix("*") match
      case "string" =>
        argName match
          case "pattern" =>
            // One capture group, so that probes with a group index find it
            Seq("'(l+)'")
          case "path" =>
            Seq("'$.a'")
          case "separator" =>
            Seq("','")
          case "field" | "unit" =>
            Seq("'month'")
          case "format" =>
            // Lockstep with the string receiver samples: '2024-03-15' is the third
            Seq("'%Y-%m-%d'")
          case "zone" =>
            Seq("'Asia/Tokyo'")
          case "search" =>
            Seq("'l'")
          case "replacement" | "pad" =>
            Seq("'*'")
          case _ =>
            Seq("'wvlet,lang'", "'42'", "'2024-03-15'", """'{"a": [1, 2]}'""", "'true'")
      case "int" | "long" | "numeric" =>
        if argName.isEmpty then
          Seq("3")
        else
          Seq("1")
      case "float" =>
        Seq("'2.5'.to_float", "'0.5'.to_float")
      case "real" | "double" =>
        Seq("'2.5'.to_double", "'0.5'.to_double")
      case "decimal" =>
        Seq("1.75")
      case "boolean" =>
        Seq("true")
      case "date" =>
        Seq("'2024-03-15'.to_date")
      case "timestamp" =>
        Seq("'2024-03-15 10:20:30'.to_timestamp")
      case "json" =>
        Seq("""'{"a": [1, 2]}'.to_json""")
      case "any" =>
        Seq("3", "'2024-03-15'")
      case "map" =>
        Seq("""map {"a": 1, "b": 2}""")
      case "A" =>
        Seq("2")
      case "K" =>
        Seq("'a'")
      case "sql" =>
        Seq("x")
      case t if t.startsWith("array") =>
        Seq("[1, 2, 3]")
      case _ =>
        Nil

  private val ArgPattern = """(\w+): ([\w\[\]*]+)""".r

  /** Candidate probe queries of a function, in the order to try */
  private def probesOf(fn: StdLibFunctionDoc): Seq[String] =
    val args =
      ArgPattern
        .findAllMatchIn(fn.signature.dropWhile(_ != '('))
        .map(m => samples(m.group(2), m.group(1)))
        .toList
    // No owner sample for `null`: its members return NULL on both engines, a vacuous match
    val receivers =
      fn.ownerType match
        case "" | "array" =>
          Seq("")
        case "string" if fn.name == "from_base64" =>
          // Generic string samples are not valid base64 of UTF-8 text
          Seq("'d3ZsZXQ='")
        case owner =>
          samples(owner)
    if receivers.isEmpty || args.exists(_.isEmpty) then
      Nil
    else
      val n = (receivers +: args).map(_.size).max
      (0 until n).flatMap { i =>
        def pick(c: Seq[String]): String = c(math.min(i, c.size - 1))
        val argList                      = args.map(pick).mkString(", ")
        val call                         =
          if args.isEmpty then
            fn.name
          else
            s"${fn.name}(${argList})"
        val windowCall = s"${fn.name}(${argList}) over (order by x)"
        fn.ownerType match
          case "" =>
            // Top-level functions, including window ranking functions
            Seq(s"select ${call} as v", s"from [[1], [2], [3]] as t(x)\nselect ${windowCall} as v")
          case "array" =>
            // Array members: aggregations over a grouped column, list functions, window functions
            Seq(
              s"from [[1], [2], [3]] as t(x)\ngroup by 'g'\nagg x.${call} as v",
              s"from [[true], [false]] as t(x)\ngroup by 'g'\nagg x.${call} as v",
              s"select [1, 2, 3].${call} as v",
              s"from [[1], [2], [3]] as t(x)\nselect x.${windowCall} as v"
            )
          case _ =>
            Seq(s"select (${pick(receivers)}).${call} as v")
      }
    end if

  end probesOf

  /** Run one probe query; Left when DuckDB or the compiler rejects the probe itself */
  private def runQuery(q: String): Either[String, (Outcome, String)] =
    val res = conformance.compareQuery(q)
    if res.message.startsWith("compile error") || res.message.startsWith("skipped") then
      Left(res.message)
    else if res.matched then
      Right((Outcome.Covered, ""))
    else if res.message.contains("Unsupported function") ||
      res.message.contains("Unsupported aggregate")
    then
      Right((Outcome.Unsupported, res.message))
    else if res.message.startsWith("engine error") then
      Right((Outcome.EngineError, res.message))
    else
      Right((Outcome.Mismatch, s"${res.message} (query: ${q.replace('\n', ' ')})"))

  private def run(fn: StdLibFunctionDoc): (Outcome, String) =
    val probes = probesOf(fn)
    if probes.isEmpty then
      (Outcome.NotProbed, "no sample for the owner or an argument type")
    else
      // The first covered probe wins; otherwise report the first probe DuckDB accepted
      val results = probes.iterator.map(q => runQuery(q)).toSeq
      results
        .collectFirst { case Right(r @ (Outcome.Covered, _)) =>
          r
        }
        .orElse(
          results.collectFirst { case Right(r) =>
            r
          }
        )
        .getOrElse(
          (
            Outcome.NotProbed,
            results
              .collect { case Left(m) =>
                m
              }
              .lastOption
              .getOrElse("")
          )
        )

  /** Stdlib functions the engine's dialect sees: dialect-neutral ones and DuckDB variants */
  private lazy val engineFunctions: Seq[StdLibFunctionDoc] = StdLibIndex
    .allFunctions
    .filter(f => f.dialects.isEmpty || f.dialects.contains("duckdb"))
    .distinctBy(f => (f.ownerType, f.signature))

  test("engine coverage of stdlib functions must not regress") {
    val results                = engineFunctions.map(f => (f, run(f)))
    val byOutcome              = results.groupBy(_._2._1)
    def count(o: Outcome): Int = byOutcome.get(o).map(_.size).getOrElse(0)
    val summary                =
      s"covered ${count(Outcome.Covered)}/${results.size} stdlib functions on the engine (${Outcome
          .values
          .filter(_ != Outcome.Covered)
          .map(o => s"${o}: ${count(o)}")
          .mkString(", ")})"
    // Covered functions last, so the gaps stay at the top of the report
    val details = (Outcome.values.filter(_ != Outcome.Covered) :+ Outcome.Covered)
      .toSeq
      .flatMap { o =>
        byOutcome
          .get(o)
          .toSeq
          .flatMap { rs =>
            s"${o} (${rs.size}):" +:
              rs.map((f, r) =>
                s"  ${f.ownerType}${
                    if f.ownerType.isEmpty then
                      ""
                    else
                      "."
                  }${f.signature}: ${r._2.replace('\n', ' ').take(160)}"
              )
          }
      }
    val report = (summary +: details).mkString("\n")
    info(report)
    val out = Path.of("target/engine-stdlib-coverage.txt")
    Files.createDirectories(out.getParent)
    Files.writeString(out, report + "\n")
    if count(Outcome.Covered) < minCovered then
      fail(s"Engine stdlib coverage regressed: ${summary} (expected >= ${minCovered})")
  }

end EngineStdlibCoverageCheck

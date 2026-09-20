/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.external

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.connector.CancellableStatement
import wvlet.lang.connector.DBConnector
import wvlet.lang.ext.ScalarFunction
import wvlet.lang.ext.TableFunction
import wvlet.lang.compiler.Name
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.runner.connector.SourceTableStaging
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.*
import wvlet.uni.log.LogSupport
import wvlet.uni.util.ULID

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import scala.util.Using

/**
  * Executes [[ExternalApply]] nodes at a materialization boundary: the child relation is evaluated
  * on the engine, its rows are handed to the function implementation (a JVM plugin, a
  * TypeScript/JavaScript module through the Node host, or an `sh"..."` command), and the returned
  * rows are loaded into a table that replaces the node before SQL generation.
  *
  * Table functions return a single result object: `rows` (or `rows_path`, a JSON-lines file)
  * becomes the relation, every other top-level field is metadata reported through `onMetadata`, and
  * an object with neither is itself a one-row relation.
  */
class ExternalFunctionExecutor(workEnv: WorkEnv, registry: ExternalFunctionRegistry)
    extends LogSupport:
  import ExternalFunctionExecutor.*

  /**
    * Replace every ExternalApply in the relation with a scan of its materialized result. Returns
    * the rewritten relation and the tables created on the engine, which the caller drops once the
    * enclosing statement finished
    */
  def materialize(
      relation: Relation,
      engine: DBConnector,
      engineName: String,
      onMetadata: (String, JSONObject) => Unit = (_, _) => (),
      register: CancellableStatement => Unit = _ => (),
      deregister: () => Unit = () => ()
  )(using ctx: Context): (Relation, List[String]) =
    val tables  = List.newBuilder[String]
    val lowered = ExternalFunctionLowering.lower(relation, registry)
    // Bottom-up, so the input of a function applied to another function's output is already a
    // scan of a materialized table
    val rewritten = lowered.transformUp { case x: ExternalApply =>
      val table = s"__wv_fn_${x.functionName.name}_${ULID.newULIDString.toLowerCase}"
      run(x, engine, engineName, table, onMetadata, register, deregister)
      tables += table
      val ref = TableRef(DoubleQuotedIdentifier(table, x.span), x.span)
      // Keep the declared schema on the scan so operators deriving their select items from
      // the input type (add, exclude, ...) still see the columns
      ref.tpe = x.schema
      ref
    }
    (rewritten.asInstanceOf[Relation], tables.result())

  private def run(
      x: ExternalApply,
      engine: DBConnector,
      engineName: String,
      table: String,
      onMetadata: (String, JSONObject) => Unit,
      register: CancellableStatement => Unit,
      deregister: () => Unit
  )(using ctx: Context): Unit =
    val name = x.functionName.name
    val args = JSONObject(
      x.args.map(a => a.name.map(_.name).getOrElse("") -> LiteralArgs.toJson(name, a.value))
    )
    if engine.dbType != wvlet.lang.compiler.DBType.DuckDB then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"External function '${name}' needs a DuckDB engine to materialize its result; '${engineName}' is ${engine
              .dbType}"
        )
    val spool = Files.createTempFile("wv_fn_rows_", ".jsonl")
    try
      def invoke(input: Iterator[String]): Long =
        val (rows, meta) = call(x, args, input, register, deregister)
        meta.foreach(m => onMetadata(name, m))
        writeRows(spool, rows)

      // The rows are spooled while the input cursor is open and loaded only after it is
      // closed, because the input and the target table live on the same connection
      val rowCount =
        (x.child, x.outputColumn) match
          case (_: EmptyRelation, None) =>
            val n = invoke(Iterator.empty)
            SourceTableStaging.loadDeclaredJsonFile(engine, table, spool, x.schema.fields)
            n
          case (child, None) =>
            val sql = GenSQL.generateSQLFromRelation(child, addHeader = false).sql
            debug(s"Input of external function ${name}:\n${sql}")
            val n = engine.streamJsonRows(sql)(invoke)
            SourceTableStaging.loadDeclaredJsonFile(engine, table, spool, x.schema.fields)
            n
          case (child, Some(out)) =>
            // Scalar map: only a row id and the argument columns leave the engine; the result
            // is joined back, so the other columns keep their exact values and types
            val sql       = GenSQL.generateSQLFromRelation(child, addHeader = false).sql
            val inTable   = s"${table}_in"
            val outTable  = s"${table}_out"
            val argList   = x.argColumns.map(c => s""""${c}"""")
            val helperIds = (rowIdColumn :: x.argColumns).map(c => s""""${c}"""").mkString(", ")
            try
              engine.execute(
                s"""create or replace table "${inTable}" as select row_number() over () as "${rowIdColumn}", * from (${sql})"""
              )
              val n = engine.streamJsonRows(
                s"""select ${(s""""${rowIdColumn}"""" :: argList).mkString(", ")} from "${inTable}""""
              )(invoke)
              SourceTableStaging.loadDeclaredJsonFile(
                engine,
                outTable,
                spool,
                Seq(
                  NamedType(Name.termName(rowIdColumn), DataType.LongType),
                  x.schema.fields.last
                )
              )
              engine.execute(
                s"""create or replace table "${table}" as select i.* exclude (${helperIds}), o."${out}" from "${inTable}" i join "${outTable}" o using ("${rowIdColumn}") order by "${rowIdColumn}""""
              )
              n
            finally
              engine.execute(s"""drop table if exists "${inTable}"""")
              engine.execute(s"""drop table if exists "${outTable}"""")
      workEnv.info(s"[function] ${name}: ${rowCount} rows")
    finally
      Files.deleteIfExists(spool)
    end try

  end run

  // Invoke the function, returning its output rows (JSON objects, one per row) and metadata
  private def call(
      x: ExternalApply,
      args: JSONObject,
      input: Iterator[String],
      register: CancellableStatement => Unit,
      deregister: () => Unit
  ): (Iterator[String], Option[JSONObject]) =
    val name                                                        = x.functionName.name
    def process[U](command: Seq[String], env: Map[String, String])(body: Path => U): U =
      ExternalProcess.run(
        label = s"function '${name}'",
        command = command,
        env = env + ("WVLET_FUNCTION_ARGS" -> args.toJSON),
        workDir = File(workEnv.path),
        input = input,
        onStderr = line => workEnv.info(s"[${name}] ${line}"),
        register = register,
        deregister = deregister
      )(body)

    if x.isShell then
      val inputColumns = JSONArray(
        x.child.relationType.fields.map(f => JSONString(f.name.name)).toIndexedSeq
      )
      val command = shellCommand(x, args)
      debug(s"Shell function ${name}: ${command}")
      process(
        Seq("sh", "-c", command),
        // Every argument is also exported as WVLET_ARG_<name>, which needs no quoting care
        // and works where `${...}` splices are unavailable (triple-quoted bodies)
        args.v.map((k, v) => s"WVLET_ARG_${k}" -> argText(v)).toMap +
          ("WVLET_INPUT_COLUMNS" -> inputColumns.toJSON)
      )(out => parseResult(name, Files.readString(out), allowJsonLines = true))
    else
      val impl = registry
        .find(name)
        .getOrElse(
          throw StatusCode
            .FUNCTION_NOT_FOUND
            .newException(
              s"No implementation of native function '${name}' was found. Provide it from a plugin jar or a TypeScript/JavaScript module in: ${registry
                  .pluginDirs
                  .map(_.getPath)
                  .mkString(", ")}"
            )
        )
      (impl, x.outputColumn) match
        case (ExternalFunctionImpl.Jvm(f: TableFunction), None) =>
          val result = f.apply(args, input.map(parseRow(name, _)))
          toRows(name, result)
        case (ExternalFunctionImpl.Jvm(f: ScalarFunction), Some(out)) =>
          val rows = input.map { line =>
            val row    = parseRow(name, line)
            val values = x.argColumns.map(c => row.get(c).map(toScala).orNull)
            val result = toJson(f.eval(values))
            JSONObject(row.v.filterNot((k, _) => x.argColumns.contains(k)) :+ (out -> result)).toJSON
          }
          // Evaluate eagerly: the input cursor closes when this call returns
          (rows.toList.iterator, None)
        case (ExternalFunctionImpl.NodeModule(_, module), None) =>
          process(registry.nodeCommandLine("table", Seq(module), Some(name)), registry.nodeEnv)(out =>
            parseResult(name, Files.readString(out), allowJsonLines = false)
          )
        case (ExternalFunctionImpl.NodeModule(_, module), Some(out)) =>
          process(
            registry.nodeCommandLine("scalar", Seq(module), Some(name)),
            registry.nodeEnv ++ Map(
              "WVLET_ARG_COLUMNS" ->
                JSONArray(x.argColumns.map(JSONString(_)).toIndexedSeq).toJSON,
              "WVLET_OUTPUT_COLUMN" -> out
            )
          )(file => (nonEmptyLines(Files.readString(file)).iterator, None))
        case (ExternalFunctionImpl.Jvm(f), mode) =>
          val expected =
            if mode.isDefined then
              "scalar"
            else
              "table"
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"Function '${name}' is used as a ${expected} function, but its implementation ${f
                  .getClass
                  .getName} is not a ${expected} function"
            )
      end match
    end if

  end call

end ExternalFunctionExecutor

object ExternalFunctionExecutor:
  // Row id that joins scalar function results back to their input rows
  private val rowIdColumn = "__wv_rid"

  private def nonEmptyLines(s: String): List[String] = s.linesIterator.filter(_.trim.nonEmpty).toList

  private def writeRows(file: Path, rows: Iterator[String]): Long =
    var count = 0L
    Using.resource(Files.newBufferedWriter(file, StandardCharsets.UTF_8)) { w =>
      rows.foreach { r =>
        w.write(r)
        w.newLine()
        count += 1
      }
    }
    count

  private def parseRow(name: String, line: String): JSONObject =
    JSON.parse(line) match
      case o: JSONObject =>
        o
      case other =>
        throw StatusCode
          .EXTERNAL_FUNCTION_FAILED
          .newException(s"Function '${name}': expected a JSON object row, got: ${other.toJSON}")

  /**
    * Parse the stdout of a function process. It is a single result object; for shell commands a
    * sequence of newline-delimited row objects is accepted too (rows only, no metadata)
    */
  private[external] def parseResult(
      name: String,
      stdout: String,
      allowJsonLines: Boolean
  ): (Iterator[String], Option[JSONObject]) =
    val lines = nonEmptyLines(stdout)
    def isObject(s: String): Boolean =
      try JSON.parse(s).isInstanceOf[JSONObject]
      catch
        case scala.util.control.NonFatal(_) =>
          false
    if lines.isEmpty then
      (Iterator.empty, None)
    else if allowJsonLines && lines.size > 1 && isObject(lines.head) then
      // A pretty-printed result object never has a complete object on its first line
      (lines.iterator, None)
    else
      val parsed =
        try JSON.parse(stdout)
        catch
          case scala.util.control.NonFatal(e) =>
            throw StatusCode
              .EXTERNAL_FUNCTION_FAILED
              .newException(
                s"Function '${name}' did not return a JSON result object (${e
                    .getMessage}): ${stdout.take(200)}",
                e
              )
      parsed match
        case o: JSONObject =>
          toRows(name, o)
        case other =>
          throw StatusCode
            .EXTERNAL_FUNCTION_FAILED
            .newException(
              s"Function '${name}' must return a JSON object, got: ${other.toJSON.take(200)}"
            )

  /** Split a result object into its rows and its metadata */
  private[external] def toRows(
      name: String,
      result: JSONObject
  ): (Iterator[String], Option[JSONObject]) =
    def metadata: Option[JSONObject] =
      val meta = result.v.filterNot((k, _) => k == "rows" || k == "rows_path")
      Option.when(meta.nonEmpty)(JSONObject(meta))

    (result.get("rows"), result.get("rows_path")) match
      case (Some(a: JSONArray), _) =>
        (a.v.iterator.map(_.toJSON), metadata)
      case (Some(other), _) =>
        throw StatusCode
          .EXTERNAL_FUNCTION_FAILED
          .newException(
            s"Function '${name}': 'rows' must be an array of objects, got: ${other.toJSON.take(200)}"
          )
      case (None, Some(JSONString(path))) =>
        (nonEmptyLines(Files.readString(Path.of(path))).iterator, metadata)
      case (None, Some(other)) =>
        throw StatusCode
          .EXTERNAL_FUNCTION_FAILED
          .newException(s"Function '${name}': 'rows_path' must be a string, got: ${other.toJSON}")
      case (None, None) =>
        // No rows: the object itself is a one-row relation
        (Iterator.single(result.toJSON), None)

  // Build the command line of an sh"..." body, splicing arguments shell-quoted
  private def shellCommand(x: ExternalApply, args: JSONObject): String =
    x.body match
      case i: InterpolatedString =>
        i.parts
          .map {
            case s: StringPart =>
              s.value
            case id: Identifier if args.get(id.leafName).isDefined =>
              shellQuote(argText(args.get(id.leafName).get))
            case other =>
              throw StatusCode
                .INVALID_ARGUMENT
                .newException(
                  s"Shell function '${x.functionName.name}' can only interpolate its parameters, found: ${other}"
                )
          }
          .mkString
      case other =>
        throw StatusCode.UNEXPECTED_STATE.newException(s"Not a shell function body: ${other}")

  // The text form of an argument handed to a shell command: strings unquoted, the rest as JSON
  private def argText(v: JSONValue): String =
    v match
      case JSONString(s) =>
        s
      case other =>
        other.toJSON

  private def shellQuote(s: String): String = s"'${s.replace("'", "'\\''")}'"

  private def toScala(v: JSONValue): Any =
    v match
      case JSONString(s) =>
        s
      case JSONLong(l) =>
        l
      case JSONDouble(d) =>
        d
      case JSONBoolean(b) =>
        b
      case _: JSONNull =>
        null
      case other =>
        other.toJSON

  private def toJson(v: Any): JSONValue =
    v match
      case null =>
        JSONNull()
      case j: JSONValue =>
        j
      case s: String =>
        JSONString(s)
      case b: Boolean =>
        JSONBoolean(b)
      case n: (Byte | Short | Int | Long) =>
        JSONLong(n.asInstanceOf[Number].longValue)
      case n: (Float | Double) =>
        JSONDouble(n.asInstanceOf[Number].doubleValue)
      case n: BigDecimal =>
        JSONDouble(n.toDouble)
      case o: Option[?] =>
        o.map(toJson).getOrElse(JSONNull())
      case other =>
        JSONString(other.toString)

end ExternalFunctionExecutor

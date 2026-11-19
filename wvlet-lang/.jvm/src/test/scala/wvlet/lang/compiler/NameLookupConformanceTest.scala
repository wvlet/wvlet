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
package wvlet.lang.compiler

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.compiler.analyzer.RelationRefResolver
import wvlet.lang.compiler.typer.TyperRules
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.IntType
import wvlet.lang.model.DataType.LongType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.Type.ErrorType
import wvlet.lang.model.Type.FunctionType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.*
import wvlet.uni.log.LogLevel
import wvlet.uni.log.Logger
import wvlet.uni.test.UniTest

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Using
import scala.util.control.NonFatal

/**
  * Replays the traces of the Lean model of name lookup (formal/WvletFormal/NameLookup, exported by
  * `lake exe name-lookup-traces`) against the shipped lookup: [[Scope]] programs,
  * [[GlobalSymbolIndex.visibleEntries]] and [[Context.findSymbolByName]] over units in several
  * packages, [[TyperRules.identifierRules]] for every symbol kind with and without a same-named
  * input column, and [[RelationRefResolver.resolveTableRef]] for every symbol kind. The model
  * proves the precedence (inner bindings shadow, the smallest basename wins, columns shadow only
  * live function definitions); this test checks that the Scala code resolves what the model
  * resolves.
  */
class NameLookupConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/compiler/lookup/name-lookup-traces.json")
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

  private def strOf(v: JSONValue): String =
    v match
      case JSONString(s) =>
        s
      case other =>
        fail(s"Expected a string: ${other.toJSON}")

  // JSON null, matched by its rendering so that the check does not depend on how the JSON
  // library represents it
  private def isNull(v: JSONValue): Boolean = v.toJSON == "null"

  private def optInt(v: JSONValue): Option[Int] =
    v match
      case v if isNull(v) =>
        None
      case JSONLong(n) =>
        Some(n.toInt)
      case other =>
        fail(s"Expected a number or null: ${other.toJSON}")

  private def cases(name: String): List[Map[String, JSONValue]] = arr(fixture(name)).map(obj)

  extension (m: Map[String, JSONValue])
    private def str(key: String): String            = strOf(m(key))
    private def strs(key: String): List[String]     = arr(m(key)).map(strOf)
    private def bool(key: String): Boolean          = m(key) == JSONBoolean(true)
    private def optStr(key: String): Option[String] = m.get(key).filterNot(isNull).map(strOf)

  private val n0 = Name.termName("n0")

  /** Report the failing trace together with the assertion that failed. */
  private def clue[A](trace: String)(body: => A): A =
    try
      body
    catch
      case NonFatal(e) =>
        fail(s"${trace}: ${e.getMessage}")

  private def newGlobal: GlobalContext = GlobalContext(
    CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv("."))
  )

  /** A symbol named `name` whose info carries no type. */
  private def namedSymbol(id: Int, name: String): Symbol =
    val sym = Symbol(id, NoSpan)
    sym.symbolInfo = SymbolInfo(
      SymbolType.ValDef,
      Symbol.NoSymbol,
      sym,
      Name.termName(name),
      DataType.UnknownType
    )
    sym

  private def qualifiedName(dotted: String): QualifiedName = dotted
    .split('.')
    .toList
    .map(UnquotedIdentifier(_, NoSpan))
    .reduceLeft[QualifiedName]((q, leaf) => DotRef(q, leaf, DataType.UnknownType, NoSpan))

  private def unitIn(path: String, pkg: Option[String]): CompilationUnit =
    val unit = CompilationUnit(SourceFile.fromString(path, ""), isPreset = pkg.isEmpty)
    pkg
      .filter(_.nonEmpty)
      .foreach { p =>
        unit.unresolvedPlan = PackageDef(qualifiedName(p), Nil, span = NoSpan)
      }
    unit

  test("replay scope programs") {
    val programs = cases("scopes")
    programs.nonEmpty shouldBe true
    programs.foreach { c =>
      val ops     = c.strs("ops")
      val handles = ArrayBuffer(Scope.newScope(0))
      val entered = List.newBuilder[Int]
      ops
        .zipWithIndex
        .foreach { (op, i) =>
          op.split(' ').toList match
            case "child" :: h :: Nil =>
              handles += handles(h.toInt).newChildScope
            case "fresh" :: Nil =>
              handles += handles(0).newFreshScope
            case "add" :: h :: n :: Nil =>
              handles(h.toInt).add(Name.termName(n), namedSymbol(i, n))
            case "enter" :: h :: n :: Nil =>
              entered += handles(h.toInt).enter(namedSymbol(i, n))(using Context.NoContext).id
            case _ =>
              fail(s"Unknown scope operation: ${op}")
        }
      val lookups =
        handles
          .map(s => List("n0", "n1").map(n => s.lookupSymbol(Name.termName(n)).map(_.id)))
          .toList
      val expected = arr(c("lookup")).map(row => arr(row).map(optInt))
      clue(s"ops ${ops.mkString(", ")}") {
        entered.result() shouldBe arr(c("entered")).map(v => optInt(v).get)
        lookups shouldBe expected
      }
    }
  }

  test("replay global lookup") {
    // Same-named definitions in two files are part of the domain; their duplicate-definition
    // warnings are expected
    val contextLogger = Logger.of[Context]
    val logLevel      = contextLogger.getLogLevel
    contextLogger.setLogLevel(LogLevel.ERROR)
    try replayGlobals()
    finally contextLogger.setLogLevel(logLevel)
  }

  private def replayGlobals(): Unit =
    val localSymbol = optInt(fixture("localSymbol")).get
    val globals     = cases("globals")
    globals.nonEmpty shouldBe true
    globals.foreach { c =>
      val global = newGlobal
      val labels = Map.newBuilder[Symbol, String]
      arr(c("units"))
        .zipWithIndex
        .foreach { (u, ui) =>
          val fields = arr(u)
          val pkg    = Option(fields(1)).filterNot(isNull).map(strOf)
          // Distinct folders, so that equal basenames are distinct files
          val unit = unitIn(s"d${ui}/${strOf(fields(0))}", pkg)
          arr(fields(2))
            .map(strOf)
            .zipWithIndex
            .foreach { (name, di) =>
              val sym = namedSymbol(1000 + ui * 10 + di, name)
              global.symbolIndex.add(unit, sym)
              labels += sym -> s"${ui}.${di}"
            }
        }
      val label   = labels.result()
      val cur     = c.str("package")
      val imports = c.strs("imports")
      val scope   = Scope.newScope(0)
      val local   = namedSymbol(localSymbol, "n0")
      if c.bool("local") then
        scope.add(n0, local)
      val ctx = Context(
        global = global,
        scope = scope,
        compilationUnit = unitIn("query.wv", Some(cur)),
        importDefs = imports.map(r => Import(qualifiedName(r), None, None, NoSpan))
      )
      def show(sym: Symbol): String =
        if sym eq local then
          s"local ${localSymbol}"
        else
          label(sym)

      clue(s"case ${c("units").toJSON} ${cur} ${imports}") {
        global.symbolIndex.visibleEntries(n0, cur, imports).map(e => label(e.symbol)) shouldBe
          c.strs("visible")
        val accepted = c.strs("found")
        ctx.findSymbolByName(n0).map(show) match
          case None =>
            accepted shouldBe Nil
          case Some(found) =>
            accepted shouldContain found
      }
    }
  end replayGlobals

  private val rowSchema = SchemaType(
    None,
    Name.typeName("row"),
    List(NamedType(Name.termName("a"), LongType))
  )

  private def one = LongLiteral(1, "1", NoSpan)

  // The relation an `as` alias stands for
  private val aliasedRelation = Values(List(ArrayConstructor(List(one), NoSpan)), rowSchema, NoSpan)

  private def infoOf(kind: String, sym: Symbol): SymbolInfo =
    val unit = CompilationUnit.empty
    kind match
      case "function" =>
        MethodSymbolInfo(
          Symbol.NoSymbol,
          sym,
          n0,
          FunctionType(n0, Nil, LongType, Nil),
          None,
          Nil,
          unit
        )
      case "value" =>
        ValSymbolInfo(Symbol.NoSymbol, sym, n0, LongType, one, unit)
      case "tableValue" =>
        ValSymbolInfo(
          Symbol.NoSymbol,
          sym,
          n0,
          rowSchema,
          ArrayConstructor(List(ArrayConstructor(List(one), NoSpan)), NoSpan),
          unit
        )
      case "model" =>
        ModelSymbolInfo(Symbol.NoSymbol, sym, n0, rowSchema, unit)
      case "alias" =>
        sym.tree = aliasedRelation
        RelationAliasSymbolInfo(Symbol.NoSymbol, sym, n0, unit)
      case "other" =>
        SavedRelationSymbolInfo(Symbol.NoSymbol, sym, n0, unit)
      case other =>
        fail(s"Unknown symbol kind: ${other}")
    end match

  end infoOf

  /**
    * Bind `n0` in a scope to a symbol of the case's kind and run `body`. For a completing symbol,
    * `body` runs inside the symbol's completer, as a self-reference does
    */
  private def withSymbol[A](c: Map[String, JSONValue], scope: Scope)(body: Option[Symbol] => A): A =
    c.optStr("symbol") match
      case None =>
        body(None)
      case Some(kind) =>
        val sym = Symbol(1, NoSpan)
        if c.bool("completing") then
          var result: Option[A] = None
          sym.setCompleter(
            n0,
            new SymbolCompleter:
              def complete(s: Symbol): SymbolInfo =
                result = Some(body(Some(sym)))
                infoOf(kind, s)
          )
          scope.add(n0, sym)
          sym.symbolInfo
          result.get
        else
          sym.symbolInfo = infoOf(kind, sym)
          scope.add(n0, sym)
          body(Some(sym))

  test("replay bare identifiers") {
    val identifiers = cases("identifiers")
    identifiers.nonEmpty shouldBe true
    identifiers.foreach { c =>
      val scope = Scope.newScope(0)
      val input =
        if c.bool("column") then
          List(NamedType(n0, IntType))
        else
          Nil
      val ctx = Context(global = newGlobal, scope = scope).withInputType(
        SchemaType(None, Name.typeName("input"), input)
      )
      val (id, sym) =
        withSymbol(c, scope) { sym =>
          val id = UnquotedIdentifier("n0", NoSpan)
          TyperRules.identifierRules(using ctx)(id)
          (id, sym)
        }
      clue(s"case ${c}") {
        c.str("result") match
          case "column" =>
            id.tpe shouldBe IntType
            id.symbol shouldBe Symbol.NoSymbol
          case "symbol" =>
            (id.symbol eq sym.get) shouldBe true
          case "unresolved" =>
            id.tpe.isInstanceOf[ErrorType] shouldBe true
          case other =>
            fail(s"Unknown identifier result: ${other}")
      }
    }
  }

  test("replay relation names") {
    val relations = cases("relations")
    relations.nonEmpty shouldBe true
    relations.foreach { c =>
      val scope = Scope.newScope(0)
      // A table declaration of the same name, which the type namespace holds
      val tableType = Symbol(2, NoSpan)
      tableType.symbolInfo = SymbolInfo(
        SymbolType.TypeDef,
        Symbol.NoSymbol,
        tableType,
        Name.typeName("n0"),
        rowSchema
      )
      scope.add(Name.typeName("n0"), tableType)
      val ctx      = Context(global = newGlobal, scope = scope)
      val ref      = TableRef(UnquotedIdentifier("n0", NoSpan), NoSpan)
      val resolved =
        withSymbol(c, scope) { _ =>
          RelationRefResolver.resolveTableRef(ref)(using ctx)
        }
      val result =
        resolved match
          case r if r eq ref =>
            "keep"
          case r if r eq aliasedRelation =>
            "alias"
          case _: ModelScan =>
            "modelScan"
          case _: Values =>
            "values"
          case _: TableScan =>
            "table"
          case other =>
            fail(s"Unexpected resolution: ${other}")
      clue(s"case ${c}") {
        result shouldBe c.str("result")
      }
    }
  }

end NameLookupConformanceTest

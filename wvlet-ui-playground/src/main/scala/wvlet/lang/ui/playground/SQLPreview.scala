package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.all.s
import wvlet.airframe.rx.{Cancelable, Rx}
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.{CompilationUnit, Compiler, SourceFile, Symbol}

class SQLPreview(currentQuery: CurrentQuery, windowSize: WindowSize, queryRunner: QueryRunner)
    extends EditorBase(windowSize, "wvlet-sql-preview", "sql"):
  override def initialText: String = "select * from lineitem\nlimit 10"

  private var monitor = Cancelable.empty

  private val compiler = Compiler.default(".")
  private val contextCompilationUnits = DemoQuerySet
    .defaultQuerySet
    .map { q =>
      CompilationUnit(SourceFile.fromString(q.name, q.query))
    }

  override def onMount: Unit =
    super.onMount
    monitor = currentQuery
      .wvletQuery
      .flatMap { newWvletQuery =>
        val unit = CompilationUnit.fromString(newWvletQuery)
        try
          val compileResult = compiler.compileMultipleUnits(contextCompilationUnits, unit)
          if !compileResult.hasFailures then
            val ctx = compileResult
              .context
              .withCompilationUnit(unit)
              .withDebugRun(false)
              .newContext(Symbol.NoSymbol)
            val sql = GenSQL.generateSQL(unit, ctx)
            setText(sql)
            queryRunner
              .runQuery("tpch", sql)
              .map { queryResult =>
                trace(s"Query result: ${queryResult}")
                currentQuery.lastQueryResult := queryResult
              }
              .tapOnFailure(e => warn(e))
          else
            Rx.empty
        catch
          case e: WvletLangException =>
            warn(e)
            // Ignore compilation errors
            Rx.empty
      }
      .subscribe()

  end onMount

  override def beforeUnmount: Unit =
    super.beforeUnmount
    monitor.cancel

end SQLPreview

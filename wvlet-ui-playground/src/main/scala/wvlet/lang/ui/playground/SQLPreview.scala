package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.all.s
import wvlet.airframe.rx.{Cancelable, Rx}
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.compiler.{CompilationUnit, Compiler, QuerySelector, SourceFile, Symbol}
import wvlet.lang.ui.component.WindowSize
import wvlet.lang.ui.component.monaco.EditorBase

class SQLPreview(currentQuery: CurrentQuery, windowSize: WindowSize, queryRunner: QueryRunner)
    extends EditorBase(
      windowSize,
      "wvlet-sql-preview",
      "sql",
      marginHeight = PlaygroundUI.editorMarginHeight
    ):
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
    editor.enableWordWrap()
    monitor = currentQuery
      .wvletQueryRequest
      .flatMap { newWvletQueryRequest =>
        val unit = CompilationUnit.fromString(newWvletQueryRequest.query)
        try
          val compileResult = compiler.compileMultipleUnits(contextCompilationUnits, unit)
          if !compileResult.hasFailures then
            val ctx = compileResult
              .context
              .withCompilationUnit(unit)
              .withDebugRun(false)
              .newContext(Symbol.NoSymbol)

            val selectedPlan = QuerySelector.selectQuery(
              unit,
              newWvletQueryRequest.linePosition,
              newWvletQueryRequest.querySelection
            )
            val sql = GenSQL.generateSQL(unit, ctx, targetPlan = Some(selectedPlan))
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
        end try
      }
      .subscribe()

  end onMount

  override def beforeUnmount: Unit =
    super.beforeUnmount
    monitor.cancel

end SQLPreview

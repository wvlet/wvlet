package com.treasuredata.flow.lang.compiler.parser

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase, SourceFile}
import com.treasuredata.flow.lang.model.plan.{LogicalPlan, PackageDef}
import org.antlr.v4.runtime.*
import wvlet.log.LogSupport

/**
  * Parse *.flow files and create untyped plans
  */
object FlowParser extends Phase("parser") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    unit.unresolvedPlan = parse(unit)
    unit

  def parseSourceFolder(path: String): Seq[LogicalPlan] =
    CompilationUnit.fromPath(path).map(parse)

  def parse(compileUnit: CompilationUnit): LogicalPlan =
    debug(s"Parsing ${compileUnit.sourceFile}")
    parse(compileUnit.sourceFile)

  def parse(sourceFile: SourceFile): LogicalPlan =
    val parser = new FlowLangParser(tokenStream(sourceFile.content))
    // Do not drop mismatched token
    parser.setErrorHandler(
      new DefaultErrorStrategy:
        override def recoverInline(recognizer: Parser): Token =
          if nextTokensContext == null then throw new InputMismatchException(recognizer)
          else throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext)
    )
    parser.removeErrorListeners()
    parser.addErrorListener(createLexerErrorListener)
    val ctx = parser.statements()
    trace(ctx.toStringTree(parser))

    val interpreter = new FlowInterpreter
    interpreter.interpret(ctx) match
      case p: PackageDef => p.copy(sourceFile = sourceFile)
      case other         => other

  private def tokenStream(text: String): CommonTokenStream =
    val lexer       = new FlowLangLexer(new ANTLRInputStream(text))
    val tokenStream = new CommonTokenStream(lexer)
    tokenStream

  private def createLexerErrorListener =
    new BaseErrorListener:
      override def syntaxError(
          recognizer: Recognizer[?, ?],
          offendingSymbol: Any,
          line: Int,
          charPositionInLine: Int,
          msg: String,
          e: RecognitionException
      ): Unit =
        throw new IllegalArgumentException(s"Lexer error at line ${line}:${charPositionInLine}. ${msg}")

package com.treasuredata.flow.lang.parser

import org.antlr.v4.runtime.{
  BaseErrorListener,
  CharStreams,
  CommonTokenStream,
  DefaultErrorStrategy,
  InputMismatchException,
  Parser,
  RecognitionException,
  Recognizer,
  Token
}
import wvlet.log.LogSupport

object FlowParser extends LogSupport:
  def parse(code: String): Unit =
    val parser = new FlowLangParser(tokenStream(code))
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
    interpreter.interpret(ctx)

  private def tokenStream(text: String): CommonTokenStream =
    val lexer       = new FlowLangLexer(new CaseInsensitiveStream(CharStreams.fromString(text)))
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

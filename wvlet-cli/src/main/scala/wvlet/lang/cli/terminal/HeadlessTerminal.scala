package wvlet.lang.cli.terminal

import wvlet.uni.cli.LineAction
import wvlet.uni.cli.LineResult
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx

import scala.io.StdIn

/**
  * Terminal for non-interactive runs (`-c` commands or an input file): plain stdin/stdout without
  * line editing, history, or key bindings
  */
class HeadlessTerminal extends REPLTerminal:
  override def width: Int  = 120
  override def height: Int = 40

  override def write(s: String): Unit = print(s)

  override def writeln(s: String): Unit = println(s)

  override def flush(): Unit = Console.flush()

  override def clearScreen(): Unit = ()

  override def printAbove(s: String): Unit = println(s)

  override def readLine(prompt: String): Rx[LineResult] = Rx.single {
    print(prompt)
    flush()
    Option(StdIn.readLine()) match
      case Some(line) =>
        LineResult.Line(line)
      case None =>
        LineResult.EndOfInput
  }

  override def isRealTerminal: Boolean = false

  // Ctrl-C is left to the JVM's default handling
  override def onInterrupt(handler: () => Unit): Cancelable = Cancelable.empty

  override def addHistory(line: String): Unit = ()

  override def bindKeys(bindings: Seq[(String, LineAction)]): Unit = ()

  override def close(): Unit = ()

end HeadlessTerminal

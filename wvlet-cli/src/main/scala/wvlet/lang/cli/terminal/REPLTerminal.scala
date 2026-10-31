package wvlet.lang.cli.terminal

import wvlet.uni.cli.LineAction
import wvlet.uni.cli.LineResult
import wvlet.uni.rx.Cancelable
import wvlet.uni.rx.Rx

/**
  * Terminal used by the REPL: [[UniTerminal]] for interactive sessions and [[HeadlessTerminal]] for
  * running commands given with `-c` or a file
  */
trait REPLTerminal extends AutoCloseable:
  /**
    * Terminal width in characters
    */
  def width: Int

  /**
    * Terminal height in lines
    */
  def height: Int

  /**
    * Write a string to the terminal
    */
  def write(s: String): Unit

  /**
    * Write a string followed by newline
    */
  def writeln(s: String): Unit

  /**
    * Flush the terminal output
    */
  def flush(): Unit

  /**
    * Clear the screen
    */
  def clearScreen(): Unit

  /**
    * Print a message above the input being edited, or just print it outside [[readLine]]
    */
  def printAbove(s: String): Unit

  /**
    * Read one input with the given prompt. Completes with `Line` on Enter, `Interrupted` on Ctrl-C,
    * or `EndOfInput` on Ctrl-D on an empty line
    */
  def readLine(prompt: String): Rx[LineResult]

  /**
    * Whether this is a real terminal (not dumb/headless)
    */
  def isRealTerminal: Boolean

  /**
    * Run the handler on Ctrl-C while no input is being read, e.g., while a query is running
    */
  def onInterrupt(handler: () => Unit): Cancelable

  /**
    * Run the body with Ctrl-C delivered as an interrupt signal to [[onInterrupt]] handlers, e.g., a
    * query run from a key binding while the input is edited in raw mode, where Ctrl-C is a key
    */
  def withInterruptSignal[A](body: => A): A = body

  /**
    * Add a line to the command history
    */
  def addHistory(line: String): Unit

  /**
    * Bind key sequences in `^X` notation (e.g., `^J^R`) to actions. Call before the first
    * [[readLine]].
    */
  def bindKeys(bindings: Seq[(String, LineAction)]): Unit

end REPLTerminal

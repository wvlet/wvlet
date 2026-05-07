package wvlet.lang.native

import wvlet.lang.cli.WvletCliCompileOption
import wvlet.lang.cli.WvletCliCompiler
import wvlet.lang.cli.WvletCliMain

/**
  * Native `wvc` CLI binary. The end-user surface (`wvc compile foo.wv`, `wvc version`, ...) is the
  * unified `WvletCliMain` shared with the JVM and Node.js builds.
  */
object WvcMain:
  def main(args: Array[String]): Unit = WvletCliMain.main(args)

  /**
    * Internal programmatic compile API, kept for [[WvcLib]] (the dynamic C library surface).
    * Accepts legacy flag-style args (`-q "query"`, `-w "folder"`, `-f "file"`, `-t "dbtype"`, `-x`)
    * and returns the generated SQL plus the `-x` (returnResult) flag. Maintained outside the
    * unified subcommand CLI because the C ABI consumers passed pre-built arg arrays before the
    * unification.
    */
  def compileWvletQuery(args: Array[String]): (String, Boolean) =
    var workFolder: String     = "."
    var query: Option[String]  = None
    var file: Option[String]   = None
    var target: Option[String] = None
    var returnResult           = false

    val it = args.iterator
    while it.hasNext do
      it.next() match
        case "-w" if it.hasNext =>
          workFolder = it.next()
        case "-q" if it.hasNext =>
          query = Some(it.next())
        case "-f" | "--file" if it.hasNext =>
          file = Some(it.next())
        case "-t" | "--target" if it.hasNext =>
          target = Some(it.next())
        case "-x" =>
          returnResult = true
        case _ =>
        // ignore unknown args; keeps behavior loose for FFI callers

    val opt = WvletCliCompileOption(
      workFolder = workFolder,
      file = file,
      query = query,
      targetDBType = target
    )
    (WvletCliCompiler(opt).generateSQL, returnResult)

  end compileWvletQuery

end WvcMain

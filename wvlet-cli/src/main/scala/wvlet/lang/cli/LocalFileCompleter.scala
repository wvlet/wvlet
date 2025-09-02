package wvlet.lang.cli

import org.jline.reader.{Candidate, Completer, LineReader, ParsedLine}
import wvlet.lang.compiler.WorkEnv

import java.io.File
import scala.jdk.CollectionConverters.*

/**
  * A simple completer that offers local filesystem paths when the cursor is inside a single-quoted
  * string literal, e.g., from 'pa<TAB>'. Paths are resolved relative to WorkEnv.path and only local
  * (non-remote) suggestions are provided.
  */
case class LocalFileCompleter(workEnv: WorkEnv) extends Completer:

  override def complete(
      reader: LineReader,
      line: ParsedLine,
      candidates: java.util.List[Candidate]
  ): Unit =
    val buf    = reader.getBuffer
    val input  = buf.toString
    val cursor = buf.cursor()

    val quoteStart =
      input.lastIndexOf('\'') match
        case -1 =>
          -1
        case i if i <= cursor =>
          i
        case _ =>
          -1

    // Only suggest when inside a single-quoted string
    val insideQuotes =
      quoteStart >= 0 &&
        (input.indexOf('\'', quoteStart + 1) match
          case -1 =>
            true
          case end =>
            cursor <= end)

    if !insideQuotes then
      return

    // Extract the in-quote prefix up to the cursor
    val afterQuote   = quoteStart + 1
    val prefixInWord = input.substring(afterQuote, cursor)

    // Resolve base directory and the leaf prefix
    val (baseDir, leafPrefix) =
      val lastSep = prefixInWord.lastIndexOf('/')
      if lastSep >= 0 then
        val dir = prefixInWord.substring(0, lastSep + 1)
        (new File(workEnv.path, dir), prefixInWord.substring(lastSep + 1))
      else
        (new File(workEnv.path), prefixInWord)

    if !baseDir.exists() || !baseDir.isDirectory then
      return

    def escapeSingleQuotes(s: String): String = s.replace("'", "''")

    val showHidden = leafPrefix.startsWith(".")
    val entries: Array[File] = Option(baseDir.listFiles())
      .getOrElse(Array.empty[File])
      .filter(f => showHidden || !f.getName.startsWith("."))
      .filter(f => f.getName.toLowerCase.startsWith(leafPrefix.toLowerCase))
      .sortBy(_.getName.toLowerCase)
      .take(200)

    // Build candidates that replace the entire quoted string with a fully quoted path
    entries.foreach { f =>
      val relPath =
        try
          val base = new File(workEnv.path).getCanonicalFile
          val full = f.getCanonicalFile
          if base == full.getParentFile then
            f.getName
          else
            base.toPath.relativize(full.toPath).toString.replace('\\', '/')
        catch
          case _: Throwable =>
            if f.getParentFile == null then
              f.getPath.replace('\\', '/')
            else
              f.getName
      val display =
        if f.isDirectory then
          s"${f.getName}/"
        else
          f.getName
      val valuePath =
        escapeSingleQuotes(relPath) +
          (if f.isDirectory && !relPath.endsWith("/") then
             "/"
           else
             "")
      // Do NOT wrap with quotes; the user is already inside a quoted string
      val replacement = valuePath
      candidates.add(
        new Candidate(
          replacement,  // value to insert (replaces the current word)
          display,      // shown label
          "files",      // group
          null,         // description
          null,         // suffix
          null,         // key
          f.isDirectory // complete only when unique; directories allow further completion
        )
      )
    }

  end complete

end LocalFileCompleter

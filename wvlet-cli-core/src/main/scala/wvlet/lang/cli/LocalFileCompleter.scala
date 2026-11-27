package wvlet.lang.cli

import wvlet.lang.compiler.WorkEnv
import wvlet.uni.cli.Candidate
import wvlet.uni.cli.Completer
import wvlet.uni.cli.Completion
import wvlet.uni.io.IO

import scala.util.control.NonFatal

/**
  * A simple completer that offers local filesystem paths when the cursor is inside a single-quoted
  * or double-quoted string literal, e.g., from 'pa<TAB>' or from "pa<TAB>". Paths are resolved
  * relative to WorkEnv.path and only local (non-remote) suggestions are provided. Works on the JVM,
  * Node.js, and Native.
  */
case class LocalFileCompleter(workEnv: WorkEnv) extends Completer:

  override def complete(input: String, cursor: Int): Completion =
    val noCompletion = Completion(cursor, Seq.empty)

    // Find the most recent quote (either single or double) before cursor
    // (a closing quote at or after the cursor is not a start)
    val singleQuoteStart = input.lastIndexOf('\'', cursor - 1)
    val doubleQuoteStart = input.lastIndexOf('"', cursor - 1)

    // Determine which type of quote we're in (if any)
    val (quoteStart, quoteChar) =
      if singleQuoteStart > doubleQuoteStart then
        (singleQuoteStart, '\'')
      else if doubleQuoteStart >= 0 then
        (doubleQuoteStart, '"')
      else
        (-1, ' ')

    // Check if cursor is inside the quoted string
    val closingQuote =
      if quoteStart >= 0 then
        input.indexOf(quoteChar, quoteStart + 1)
      else
        -1
    val insideQuotes = quoteStart >= 0 && (closingQuote == -1 || cursor <= closingQuote)

    if !insideQuotes then
      noCompletion
    else
      // Extract the in-quote prefix up to the cursor
      val afterQuote   = quoteStart + 1
      val prefixInWord = input.substring(afterQuote, cursor)

      // Resolve the typed directory (relative to the work folder) and the leaf prefix
      val (dir, leafPrefix) =
        val lastSep = prefixInWord.lastIndexOf('/')
        if lastSep >= 0 then
          (prefixInWord.substring(0, lastSep + 1), prefixInWord.substring(lastSep + 1))
        else
          ("", prefixInWord)
      val baseDir =
        if dir.startsWith("/") then
          IO.path(dir)
        else if dir.isEmpty then
          IO.path(workEnv.path)
        else
          IO.path(workEnv.path, dir)

      if !IO.isDirectory(baseDir) then
        noCompletion
      else
        def escapeQuotes(s: String, quoteChar: Char): String =
          quoteChar match
            case '\'' =>
              s.replace("'", "''")
            case '"' =>
              s.replace("\"", "\\\"")
            case _ =>
              s

        val showHidden = leafPrefix.startsWith(".")
        val entries    =
          (
            try
              IO.list(baseDir)
            catch
              case NonFatal(_) =>
                Seq.empty
          ).map(_.fileName)
            .filter(name => showHidden || !name.startsWith("."))
            .filter(name => name.toLowerCase.startsWith(leafPrefix.toLowerCase))
            .sortBy(_.toLowerCase)
            .take(200)

        // Build candidates for each entry, keeping the directory as typed
        val candidates = entries.map { name =>
          val isDir   = IO.isDirectory(baseDir / name)
          val relPath = s"${dir}${name}"
          val display =
            if isDir then
              s"${name}/"
            else
              name
          val valuePath =
            escapeQuotes(relPath, quoteChar) + (
              if isDir then
                "/"
              else
                ""
            )
          if isDir || closingQuote >= 0 then
            // Keep typing into a directory, or before the closing quote that is already there
            Candidate(valuePath, display, complete = false)
          else
            // Close the quote. A complete candidate is followed by a space
            Candidate(s"${valuePath}${quoteChar}", display, complete = true)
        }
        // Replace the in-quote text before the cursor with the candidate path
        Completion(afterQuote, candidates.toSeq)
      end if
    end if

  end complete

end LocalFileCompleter

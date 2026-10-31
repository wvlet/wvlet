package wvlet.lang.cli

import wvlet.lang.compiler.WorkEnv
import wvlet.uni.cli.Candidate
import wvlet.uni.cli.Completer
import wvlet.uni.cli.Completion

import java.io.File

/**
  * A simple completer that offers local filesystem paths when the cursor is inside a single-quoted
  * or double-quoted string literal, e.g., from 'pa<TAB>' or from "pa<TAB>". Paths are resolved
  * relative to WorkEnv.path and only local (non-remote) suggestions are provided.
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

      // Resolve base directory and the leaf prefix
      val (baseDir, leafPrefix) =
        val lastSep = prefixInWord.lastIndexOf('/')
        if lastSep >= 0 then
          val dir = prefixInWord.substring(0, lastSep + 1)
          (new File(workEnv.path, dir), prefixInWord.substring(lastSep + 1))
        else
          (new File(workEnv.path), prefixInWord)

      if !baseDir.exists() || !baseDir.isDirectory then
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

        val showHidden           = leafPrefix.startsWith(".")
        val entries: Array[File] = Option(baseDir.listFiles())
          .getOrElse(Array.empty[File])
          .filter(f => showHidden || !f.getName.startsWith("."))
          .filter(f => f.getName.toLowerCase.startsWith(leafPrefix.toLowerCase))
          .sortBy(_.getName.toLowerCase)
          .take(200)

        // Precompute canonical bases once (avoid repeating in loop)
        val (workBase, baseCanonical) =
          val wb =
            try
              new File(workEnv.path).getCanonicalFile
            catch
              case _: Throwable =>
                new File(workEnv.path).getAbsoluteFile
          val bc =
            try
              baseDir.getCanonicalFile
            catch
              case _: Throwable =>
                baseDir.getAbsoluteFile
          (wb, bc)

        // Build candidates for each entry
        val candidates = entries.map { f =>
          val relPath =
            try
              val full =
                try
                  f.getCanonicalFile
                catch
                  case _: Throwable =>
                    f.getAbsoluteFile
              if baseCanonical == workBase then
                // Base is the work directory; keep just the name
                f.getName
              else
                workBase.toPath.relativize(full.toPath).toString.replace('\\', '/')
            catch
              case _: Throwable =>
                Option(f.getParentFile) match
                  case None =>
                    f.getPath.replace('\\', '/')
                  case Some(_) =>
                    f.getName
          val display =
            if f.isDirectory then
              s"${f.getName}/"
            else
              f.getName
          val valuePath =
            escapeQuotes(relPath, quoteChar) + (
              if f.isDirectory && !relPath.endsWith("/") then
                "/"
              else
                ""
            )
          if f.isDirectory || closingQuote >= 0 then
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

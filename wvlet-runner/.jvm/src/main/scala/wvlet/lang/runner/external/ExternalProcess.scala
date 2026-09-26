/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.external

import wvlet.lang.api.StatusCode
import wvlet.lang.connector.CancellableStatement
import wvlet.uni.log.LogSupport

import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import scala.collection.mutable

/**
  * Runs a subprocess that implements an external function: input rows are written to its stdin as
  * JSON lines, its stdout is spooled to a file handed to `body`, and its stderr is forwarded line
  * by line to `onStderr`. A non-zero exit code fails with the tail of stderr in the message.
  */
object ExternalProcess extends LogSupport:
  private val stderrTailLines         = 20
  private val heartbeatRows           = 10000L
  private val heartbeatIntervalMillis = 1000L

  def run[U](
      label: String,
      command: Seq[String],
      env: Map[String, String],
      workDir: File,
      input: Iterator[String],
      onStderr: String => Unit,
      register: CancellableStatement => Unit,
      deregister: () => Unit,
      heartbeat: () => Unit = () => ()
  )(body: Path => U): U =
    val stdoutFile = Files.createTempFile("wv_fn_out_", ".json")
    try
      val pb = ProcessBuilder(command*)
      pb.directory(workDir)
      env.foreach((k, v) => pb.environment().put(k, v))
      val process =
        try
          pb.start()
        catch
          case e: IOException =>
            throw StatusCode
              .EXTERNAL_FUNCTION_FAILED
              .newException(s"Failed to start ${label}: ${e.getMessage}", e)
      // Lets a flow timeout or cancellation stop the function while it is running
      register(
        new CancellableStatement:
          override def cancel(): Unit = process.destroyForcibly()
      )
      val stderrTail = mutable.Queue.empty[String]
      val stdoutPump =
        daemon(s"${label}-stdout") {
          Files.copy(process.getInputStream, stdoutFile, StandardCopyOption.REPLACE_EXISTING)
        }
      val stderrPump =
        daemon(s"${label}-stderr") {
          scala
            .io
            .Source
            .fromInputStream(process.getErrorStream, "UTF-8")
            .getLines()
            .foreach { line =>
              stderrTail.synchronized {
                stderrTail.enqueue(line)
                if stderrTail.size > stderrTailLines then
                  stderrTail.dequeue()
              }
              onStderr(line)
            }
        }
      try
        // Feed the rows on the calling thread: the iterator is usually backed by an open
        // database cursor that must not be shared across threads
        try
          val out = process.outputWriter(StandardCharsets.UTF_8)
          try
            var written = 0L
            input.foreach { row =>
              out.write(row)
              out.write('\n')
              written += 1
              if written % heartbeatRows == 0 then
                heartbeat()
            }
          finally
            out.close()
        catch
          case _: IOException =>
          // The function exited (or closed stdin) without reading all input; its exit code
          // below decides whether that is a failure

        // Report liveness while the function works, so a stage `heartbeat:` watchdog does not
        // mistake a long-running function for a stalled one
        while !process.waitFor(heartbeatIntervalMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
        do
          heartbeat()
        val exitCode = process.exitValue()
        stdoutPump.join()
        stderrPump.join()
        if exitCode != 0 then
          val tail = stderrTail.synchronized(stderrTail.mkString("\n"))
          throw StatusCode
            .EXTERNAL_FUNCTION_FAILED
            .newException(
              s"${label} failed with exit code ${exitCode}${
                  if tail.isEmpty then
                    ""
                  else
                    s":\n${tail}"
                }"
            )
        body(stdoutFile)
      catch
        case e: InterruptedException =>
          process.destroyForcibly()
          throw e
      finally
        if process.isAlive then
          process.destroyForcibly()
        deregister()
      end try
    finally
      Files.deleteIfExists(stdoutFile)
    end try

  end run

  private def daemon(name: String)(body: => Unit): Thread =
    val t = Thread(
      () =>
        try
          body
        catch
          case e: IOException =>
            debug(s"${name}: ${e.getMessage}")
      ,
      name
    )
    t.setDaemon(true)
    t.start()
    t

end ExternalProcess

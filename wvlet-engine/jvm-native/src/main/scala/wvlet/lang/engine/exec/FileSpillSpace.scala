/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.engine.exec

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.*

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

/**
  * Spill files in `<parent>/wvlet-spill-<random>/`, created on the first file and readable by its
  * owner only. [[close]] deletes the files this space created, then the folder; a shutdown hook
  * closes the spaces still open when the process exits
  */
final class FileSpillSpace(parent: String) extends SpillSpace:
  @volatile
  private var dir: File = null

  @volatile
  private var closed = false

  private val counter = AtomicInteger(0)
  private val written = AtomicLong(0L)
  private val files   = ConcurrentHashMap.newKeySet[FileSpillFile]()

  def bytesWritten: Long = written.get()

  private def folder: File = synchronized {
    if closed then
      throw StatusCode.RESOURCE_EXHAUSTED.newException("The query's spill files were closed")
    if dir == null then
      val base = File(parent)
      if !base.isDirectory && !base.mkdirs() && !base.isDirectory then
        throw StatusCode
          .RESOURCE_EXHAUSTED
          .newException(s"Cannot create the spill folder ${base.getPath}")
      var d     = File(base, f"wvlet-spill-${Random.nextLong()}%016x")
      var tries = 0
      while !d.mkdir() do
        tries += 1
        if tries > 10 then
          throw StatusCode
            .RESOURCE_EXHAUSTED
            .newException(s"Cannot create a spill folder in ${base.getPath}")
        d = File(base, f"wvlet-spill-${Random.nextLong()}%016x")
      FileSpillSpace.ownerOnly(d)
      dir = d
      FileSpillSpace.open.add(this)
    dir
  }

  def newFile(schema: Schema): SpillFile =
    val f    = File(folder, s"part-${counter.getAndIncrement()}.spill")
    val file = FileSpillFile(this, f, schema)
    files.add(file)
    if closed then
      file.delete()
      throw StatusCode.RESOURCE_EXHAUSTED.newException("The query's spill files were closed")
    file

  private[exec] def checkOpen(): Unit =
    if closed then
      throw StatusCode.RESOURCE_EXHAUSTED.newException("The query's spill files were closed")

  private[exec] def wrote(bytes: Long): Unit = written.addAndGet(bytes)

  private[exec] def deleted(f: FileSpillFile): Unit = files.remove(f)

  def close(): Unit =
    val d = synchronized {
      closed = true
      dir
    }
    if d != null then
      files.forEach(_.delete())
      files.clear()
      // Only the files this space created: anything else in the folder stays, with the folder
      d.delete()
      FileSpillSpace.open.remove(this)

  override def toString: String = s"FileSpillSpace(${Option(dir).map(_.getPath).getOrElse(parent)})"

end FileSpillSpace

object FileSpillSpace:
  private val open = ConcurrentHashMap.newKeySet[FileSpillSpace]()

  // Spill folders of queries still running when the process exits
  Runtime
    .getRuntime
    .addShutdownHook(
      Thread(() =>
        open.forEach { s =>
          try
            s.close()
          catch
            case _: Throwable =>
        }
      )
    )

  private def ownerOnly(d: File): Unit =
    d.setReadable(false, false)
    d.setWritable(false, false)
    d.setExecutable(false, false)
    d.setReadable(true, true)
    d.setWritable(true, true)
    d.setExecutable(true, true)

/** One spill file of a [[FileSpillSpace]]: length-prefixed [[SpillBlocks]] */
final class FileSpillFile private[exec] (space: FileSpillSpace, file: File, val schema: Schema)
    extends SpillFile:
  private var out: DataOutputStream = null
  private var finished              = false
  @volatile
  private var deleted = false

  private val readers = ConcurrentHashMap.newKeySet[java.io.Closeable]()

  private var rowCount    = 0L
  private var memoryBytes = 0L
  // The file position of each block (after its length prefix), its length, and its rows
  private val offsets  = scala.collection.mutable.ArrayBuffer.empty[Long]
  private val lengths  = scala.collection.mutable.ArrayBuffer.empty[Int]
  private val rowsOf   = scala.collection.mutable.ArrayBuffer.empty[Int]
  private var position = 0L

  def rows: Long           = rowCount
  def estimatedBytes: Long = memoryBytes

  def appendBlock(block: Array[Byte], rows: Int, estimatedBytes: Long): Unit =
    require(!finished, "Appending to a finished spill file")
    space.checkOpen()
    if out == null then
      out = DataOutputStream(BufferedOutputStream(FileOutputStream(file), 1 << 16))
    out.writeInt(block.length)
    out.write(block)
    space.wrote(4L + block.length)
    offsets += position + 4
    lengths += block.length
    rowsOf += rows
    position += 4L + block.length
    rowCount += rows
    memoryBytes += estimatedBytes

  def finish(): Unit =
    if !finished then
      finished = true
      if out != null then
        out.close()
        out = null

  def read(): Iterator[Batch] =
    require(finished, "Reading an unfinished spill file")
    space.checkOpen()
    if rowCount == 0 || deleted then
      return Iterator.empty
    val in = DataInputStream(BufferedInputStream(FileInputStream(file), 1 << 16))
    readers.add(in)
    def close(): Unit =
      readers.remove(in)
      in.close()
    new Iterator[Batch]:
      private var nextBatch: Batch = advance()

      private def advance(): Batch =
        try
          val len =
            try
              in.readInt()
            catch
              case _: EOFException =>
                -1
          if len < 0 then
            close()
            null
          else
            space.checkOpen()
            val bytes = new Array[Byte](len)
            in.readFully(bytes)
            SpillBlocks.decode(schema, bytes)
        catch
          case e: Throwable =>
            close()
            throw e

      def hasNext: Boolean = nextBatch != null

      def next(): Batch =
        val b = nextBatch
        nextBatch = advance()
        b
    end new

  end read

  def blockRows: IndexedSeq[Int] =
    require(finished, "Reading an unfinished spill file")
    rowsOf.toIndexedSeq

  def readBlocks(from: Int, until: Int): Iterator[Batch] =
    require(finished, "Reading an unfinished spill file")
    space.checkOpen()
    // A shared result's readers read its file until its last one finished: a read after that
    // must fail rather than return no rows
    if deleted then
      throw StatusCode
        .UNEXPECTED_STATE
        .newException(s"Reading spill file ${file.getName} after it was deleted")
    if from >= until then
      return Iterator.empty
    val in = RandomAccessFile(file, "r")
    readers.add(in)
    def close(): Unit =
      readers.remove(in)
      in.close()
    new Iterator[Batch]:
      private var i = from

      def hasNext: Boolean =
        if i < until then
          true
        else
          if i == until then
            i += 1
            close()
          false

      def next(): Batch =
        if i >= until then
          throw java.util.NoSuchElementException("No more spill blocks")
        try
          space.checkOpen()
          val bytes = new Array[Byte](lengths(i))
          in.seek(offsets(i))
          in.readFully(bytes)
          i += 1
          SpillBlocks.decode(schema, bytes)
        catch
          case e: Throwable =>
            i = until + 1
            close()
            throw e
    end new

  end readBlocks

  def delete(): Unit =
    if !deleted then
      deleted = true
      if out != null then
        try
          out.close()
        catch
          case _: Throwable =>
        out = null
      // Readers abandoned midway (a failed partition) would keep the file open
      readers.forEach { in =>
        try
          in.close()
        catch
          case _: Throwable =>
      }
      readers.clear()
      file.delete()
      space.deleted(this)

  override def toString: String = s"FileSpillFile(${file.getName}, rows=${rowCount})"

end FileSpillFile

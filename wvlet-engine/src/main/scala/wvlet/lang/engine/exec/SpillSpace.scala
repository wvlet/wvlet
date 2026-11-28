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

import wvlet.lang.engine.vector.*

/**
  * The spill files of one query, in a folder of its own that [[close]] deletes. Created on the
  * query's first spill ([[MemoryContext.spillSpace]]) and closed with its memory context
  */
trait SpillSpace:
  /** A new empty file for batches of `schema` */
  def newFile(schema: Schema): SpillFile

  /** Bytes written to this space's files so far */
  def bytesWritten: Long

  /** Delete every file and the folder; files opened afterwards fail. Idempotent */
  def close(): Unit

/**
  * Batches appended to one file of a [[SpillSpace]], then read back: written by one thread, and
  * read after [[finish]], possibly more than once
  */
trait SpillFile:
  def schema: Schema

  /** Append the rows of `b` (deferred columns are gathered) */
  def append(b: Batch): Unit =
    val c = b.compact()
    appendBlock(SpillBlocks.encode(c), c.size, MemoryContext.estimatedBytes(c))

  /**
    * Append a block [[SpillBlocks.encode]] made (e.g. on another thread) of `rows` rows and
    * `estimatedBytes` in memory
    */
  def appendBlock(block: Array[Byte], rows: Int, estimatedBytes: Long): Unit

  /** Close the writer: no more appends */
  def finish(): Unit

  /** The rows appended */
  def rows: Long

  /** The estimated in-memory bytes of the appended batches ([[MemoryContext.estimatedBytes]]) */
  def estimatedBytes: Long

  /** The appended batches, in order, read from disk as the iterator advances */
  def read(): Iterator[Batch]

  /** Delete the file. Idempotent */
  def delete(): Unit

/** How each platform spills (no spilling on Scala.js) */
trait SpillCompat:
  /** Whether queries can spill to files on this platform */
  def isSupported: Boolean

  /**
    * The spill threshold of an engine that sets none: half the max heap on the JVM, none (never
    * spill) on Native, whose heap has no cap by default, and on Scala.js
    */
  def defaultThresholdBytes: Long

  /** The folder under which queries create their spill folders by default */
  def defaultDirectory: String

  /** A spill space in a new folder under `directory`, created on its first file */
  def newSpace(directory: String): SpillSpace

object SpillCompat extends SpillCompatImpl

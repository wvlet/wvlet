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
package wvlet.lang.tablestore.objectstore

import wvlet.uni.io.IO
import java.io.InputStream

case class ObjectSummary(key: String, byteSize: Long)

/** Result of a verified put */
case class PutResult(key: String, checksum: String, byteSize: Long)

/**
  * Immutable object storage for data files: checksummed puts, streaming gets, and inventory list.
  * Implementations cover local filesystems today; S3/GCS/Azure drivers slot in behind this trait
  * without protocol changes (encryption etc. is deployment configuration, not protocol).
  */
trait ObjectStore extends AutoCloseable:

  /** Store bytes at `key`, verifying integrity with the SHA-256 checksum of the payload */
  def put(key: String, bytes: Array[Byte]): PutResult

  def get(key: String): Array[Byte] = withStream(key)(readAll)

  /** Stream the object content. The stream must not be used after the method returns */
  def withStream[A](key: String)(f: InputStream => A): A

  /**
    * Run `f` against a local file holding the object content. Local stores hand out the real file
    * (treat it as read-only); remote stores stage a temporary copy.
    */
  def withLocalFile[A](key: String)(f: java.io.File => A): A

  def exists(key: String): Boolean

  /** List objects under a key prefix — used by orphan detection, never as a routine write path */
  def list(prefix: String): Seq[ObjectSummary]

  def delete(key: String): Unit

  private def readAll(in: InputStream): Array[Byte] =
    val out = new java.io.ByteArrayOutputStream()
    val buf = new Array[Byte](8192)
    var n   = 0
    while
      n = in.read(buf)
      n != -1
    do
      out.write(buf, 0, n)
    out.toByteArray

end ObjectStore

object ObjectStore:
  def local(rootPath: String): LocalObjectStore = LocalObjectStore(rootPath)

/**
  * Filesystem-backed [[ObjectStore]]. Keys are slash-separated relative paths resolved under the
  * root directory; path traversal outside the root is rejected.
  */
class LocalObjectStore(rootPath: String) extends ObjectStore:
  import java.io.File

  // Canonicalize once so symlinked roots (e.g. macOS /tmp -> /private/tmp) don't false-positive
  // the containment check
  private val root: File =
    val dir = File(rootPath)
    IO.createDirectoryIfNotExists(dir.getPath)
    dir.getCanonicalFile

  private def pathOf(key: String): File =
    val f = File(root, key).getCanonicalFile
    if !f.getPath.startsWith(root.getPath) then
      throw new IllegalArgumentException(s"Object key escapes the store root: ${key}")
    f.getParentFile match
      case p if p != null =>
        IO.createDirectoryIfNotExists(p.getPath)
      case _ =>
    f

  override def put(key: String, bytes: Array[Byte]): PutResult =
    val f        = pathOf(key)
    val checksum = Checksum.sha256Hex(bytes)
    // Write to a temp file first so a crashed writer never leaves a partial object at `key`
    val tmp = File(f.getParentFile, s".${f.getName}.tmp-${java.util.UUID.randomUUID()}")
    try
      IO.writeBytes(tmp.getPath, bytes)
      if !tmp.renameTo(f) then
        // Cross-device or platform quirks: fall back to copy + delete
        IO.copy(tmp.getPath, f.getPath)
        tmp.delete()
    finally
      tmp.delete()
    PutResult(key, checksum, bytes.length)

  override def withStream[A](key: String)(f: InputStream => A): A =
    val in = new java.io.BufferedInputStream(new java.io.FileInputStream(pathOf(key)))
    try f(in)
    finally in.close()

  override def withLocalFile[A](key: String)(f: java.io.File => A): A = f(pathOf(key))

  override def exists(key: String): Boolean = pathOf(key).isFile

  override def list(prefix: String): Seq[ObjectSummary] =
    val dir = File(root, prefix)
    if !dir.isDirectory then
      Nil
    else
      IO.list(dir.getPath)
        .flatMap { entry =>
          val childKey = s"${prefix.stripSuffix("/")}/${entry.fileName}"
          if IO.info(entry.path).fileType == wvlet.uni.io.FileType.Directory then
            list(childKey)
          else
            Seq(ObjectSummary(childKey, IO.info(entry).size))
        }
        .toSeq

  override def delete(key: String): Unit =
    val f = pathOf(key)
    if f.exists() then
      IO.deleteIfExists(f.getPath)

  override def close(): Unit = {}
end LocalObjectStore

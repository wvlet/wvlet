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

/**
  * The JVM spills to files, by default past half the max heap (less a per-thread allowance with
  * `spill_headroom`), or past the bytes of the wvlet.engine.spillThreshold system property. The
  * wvlet.engine.spillThreadAllowance property sets the per-thread allowance in bytes
  */
trait SpillCompatImpl extends SpillCompat:
  def isSupported: Boolean = true

  private def setThreshold: Option[Long] = sys
    .props
    .get("wvlet.engine.spillThreshold")
    .flatMap(_.trim.toLongOption)

  def defaultThresholdBytes: Long =
    val max = Runtime.getRuntime.maxMemory()
    setThreshold.getOrElse {
      if max == Long.MaxValue then
        Long.MaxValue
      else
        max / 2
    }

  override def headroomThresholdBytes(threads: Int): Long = setThreshold.getOrElse {
    val allowance = sys
      .props
      .get("wvlet.engine.spillThreadAllowance")
      .flatMap(_.trim.toLongOption)
      .getOrElse(SpillThreshold.defaultThreadAllowanceBytes)
    SpillThreshold.withHeadroom(Runtime.getRuntime.maxMemory(), threads, allowance)
  }

  def defaultDirectory: String = System.getProperty("java.io.tmpdir")

  def newSpace(directory: String): SpillSpace = FileSpillSpace(directory)

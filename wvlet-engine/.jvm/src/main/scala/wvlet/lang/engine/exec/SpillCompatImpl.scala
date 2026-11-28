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

  def defaultScanDecodeBudgetBytes: Long =
    SpillCompatImpl.heapShare("wvlet.engine.scanDecodeBudget")(max =>
      math.max(max / 8, max - SpillCompatImpl.DecodeReserveBytes)
    )

  def defaultDirectory: String = System.getProperty("java.io.tmpdir")

  def newSpace(directory: String): SpillSpace = FileSpillSpace(directory)

end SpillCompatImpl

object SpillCompatImpl:
  /**
    * The heap the default scan decode budget leaves to everything else on a large heap. On spark1
    * at TPC-H SF10 (1M-row row groups), -Xmx768m ran out of heap with a 192 MB budget and finished
    * q4 and q6 with 128 MB; leaving 2 GB slowed 2g 2.25x and 1536m 2.4x on q1, while leaving 1 GB
    * gave 1536m 21 of 22 queries and 2g 1.13x
    */
  val DecodeReserveBytes: Long = 1L << 30

  /** The bytes of system property `property`, or `share` of the max heap (none without a cap) */
  def heapShare(property: String)(share: Long => Long): Long = sys
    .props
    .get(property)
    .flatMap(_.trim.toLongOption)
    .getOrElse {
      val max = Runtime.getRuntime.maxMemory()
      if max == Long.MaxValue then
        Long.MaxValue
      else
        share(max)
    }

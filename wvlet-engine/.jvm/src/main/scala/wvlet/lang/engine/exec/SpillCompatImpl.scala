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
  * The JVM spills to files, by default past half the max heap, or past the bytes of the
  * wvlet.engine.spillThreshold system property
  */
trait SpillCompatImpl extends SpillCompat:
  def isSupported: Boolean = true

  def defaultThresholdBytes: Long =
    val max = Runtime.getRuntime.maxMemory()
    val set = sys.props.get("wvlet.engine.spillThreshold").flatMap(_.trim.toLongOption)
    if set.isDefined then
      return set.get
    if max == Long.MaxValue then
      Long.MaxValue
    else
      max / 2

  def defaultDirectory: String = System.getProperty("java.io.tmpdir")

  def newSpace(directory: String): SpillSpace = FileSpillSpace(directory)

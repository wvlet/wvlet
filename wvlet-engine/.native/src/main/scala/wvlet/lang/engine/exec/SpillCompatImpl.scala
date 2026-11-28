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
  * Native spills to files once a threshold is set: its heap has no cap by default
  * (`GC_MAXIMUM_HEAP_SIZE`), so there is no default threshold
  */
trait SpillCompatImpl extends SpillCompat:
  def isSupported: Boolean        = true
  def defaultThresholdBytes: Long = Long.MaxValue
  def defaultDirectory: String    = Option(System.getProperty("java.io.tmpdir"))
    .orElse(sys.env.get("TMPDIR"))
    .getOrElse("/tmp")

  def newSpace(directory: String): SpillSpace = FileSpillSpace(directory)

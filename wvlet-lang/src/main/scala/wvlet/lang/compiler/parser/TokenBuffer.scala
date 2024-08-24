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
package wvlet.lang.compiler.parser

import wvlet.log.LogSupport

class TokenBuffer(initialSize: Int = 1024) extends LogSupport:
  private var buf: Array[Char] = new Array[Char](initialSize)
  private var len: Int         = 0

  def append(ch: Char): Unit =
    if len == buf.length then
      // Double the buffer size
      val newBuffer = new Array[Char](buf.length * 2)
      Array.copy(buf, 0, newBuffer, 0, len)
      buf = newBuffer
    buf(len) = ch
    len += 1
  end append

  def length            = len
  def isEmpty: Boolean  = len == 0
  def nonEmpty: Boolean = !isEmpty
  def clear(): Unit     = len = 0
  def last: Char        = buf(len - 1)

  override def toString: String = new String(buf, 0, len)

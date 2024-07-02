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
package wvlet.lang.compiler.ast

import Span.*

/**
  * Span represents a range of source code [start, end)
  * @param start
  *   The start position of the span from the beginning of the source file
  * @param end
  *   The end position of the span from the beginning of the source file
  * @param pointOffset
  *   If given, the offset within a span
  */
class Span(start: Int, end: Int, pointOffset: Option[Int] = None):
  def exists: Boolean = this != NoSpan

object Span:
  // Non-existing span
  object NoSpan extends Span(1, 0)

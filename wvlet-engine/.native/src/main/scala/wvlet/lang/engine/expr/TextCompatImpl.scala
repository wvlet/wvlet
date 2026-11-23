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
package wvlet.lang.engine.expr

import wvlet.lang.api.StatusCode

/** Scala Native has no Unicode normalizer: ASCII text is already in NFC, other text is rejected */
trait TextCompatImpl extends TextCompat:
  def nfcNormalize(s: String): String =
    if s.forall(_ < 0x80) then
      s
    else
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException("normalize of non-ASCII text is not supported in the native engine")

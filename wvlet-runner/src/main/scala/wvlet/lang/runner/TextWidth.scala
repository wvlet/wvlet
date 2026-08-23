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
package wvlet.lang.runner

/**
  * Cross-platform display-cell width of a character (Markus Kuhn's `wcwidth` algorithm). Replaces
  * jline's `WCWidth` (JVM-only) so the table printer renders identically on JVM, Node.js, and
  * Native. Combining marks are detected through `Character.getType`, which all three platforms
  * implement with full Unicode tables; wide (2-cell) ranges follow Kuhn's East-Asian list for the
  * BMP — the printer only measures `Char`s, so supplementary planes don't arise here.
  */
private[runner] object TextWidth:

  def wcwidth(ch: Char): Int =
    if ch == '\u0000' then
      0
    else if ch < 32 || (ch >= 0x7f && ch < 0xa0) then
      -1
    else if isZeroWidth(ch) then
      0
    else if isWide(ch) then
      2
    else
      1

  private def isZeroWidth(ch: Char): Boolean =
    val t = Character.getType(ch)
    t == Character.NON_SPACING_MARK || t == Character.ENCLOSING_MARK ||
    // Zero-width/formatting controls (ZWSP, ZWJ, bidi marks, …) — but the soft hyphen prints
    (t == Character.FORMAT && ch != '\u00ad')

  private def isWide(ch: Char): Boolean =
    ch >= 0x1100 && (
      ch <= 0x115f ||                                     // Hangul Jamo init. consonants
        ch == 0x2329 || ch == 0x232a ||                   // angle brackets
        (ch >= 0x2e80 && ch <= 0xa4cf && ch != 0x303f) || // CJK ... Yi
        (ch >= 0xac00 && ch <= 0xd7a3) ||                 // Hangul Syllables
        (ch >= 0xf900 && ch <= 0xfaff) ||                 // CJK Compatibility Ideographs
        (ch >= 0xfe10 && ch <= 0xfe19) ||                 // Vertical forms
        (ch >= 0xfe30 && ch <= 0xfe6f) ||                 // CJK Compatibility Forms
        (ch >= 0xff00 && ch <= 0xff60) ||                 // Fullwidth Forms
        (ch >= 0xffe0 && ch <= 0xffe6)
    )

end TextWidth

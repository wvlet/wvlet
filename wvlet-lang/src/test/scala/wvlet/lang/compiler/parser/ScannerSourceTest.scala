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

import wvlet.airspec.AirSpec

class ScannerSourceTest extends AirSpec:

  test("find line and column positions") {
    val src = ScannerSource.fromText("a\nb\nc")
    src.length shouldBe 5

    test("find line index") {
      src.offsetToLine(0) shouldBe 0
      src.offsetToLine(1) shouldBe 0
      src.offsetToLine(2) shouldBe 1
      src.offsetToLine(3) shouldBe 1
      src.offsetToLine(4) shouldBe 2
    }

    test("find column index") {
      src.offsetToColumn(0) shouldBe 1
      src.offsetToColumn(1) shouldBe 2
      src.offsetToColumn(2) shouldBe 1
      src.offsetToColumn(3) shouldBe 2
      src.offsetToColumn(4) shouldBe 1
    }

    test("find startOfLine") {
      src.startOfLine(0) shouldBe 0
      src.startOfLine(1) shouldBe 0
      src.startOfLine(2) shouldBe 2
      src.startOfLine(3) shouldBe 2
      src.startOfLine(4) shouldBe 4
    }
  }

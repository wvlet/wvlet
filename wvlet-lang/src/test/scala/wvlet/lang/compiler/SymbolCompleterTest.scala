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
package wvlet.lang.compiler

import wvlet.uni.test.UniTest
import wvlet.lang.api.Span
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.model.DataType

class SymbolCompleterTest extends UniTest:

  private def newSymbolInfo(sym: Symbol, name: String): SymbolInfo = SymbolInfo(
    SymbolType.ValDef,
    Symbol.NoSymbol,
    sym,
    Name.termName(name),
    DataType.LongType
  )

  private def setCompleter(sym: Symbol, name: String)(f: Symbol => SymbolInfo): Unit = sym
    .setCompleter(
      Name.termName(name),
      new SymbolCompleter:
        override def complete(symbol: Symbol): SymbolInfo = f(symbol)
    )

  test("should return NoSymbolInfo for a symbol without info or completer") {
    val sym = Symbol(1, Span.NoSpan)
    sym.hasSymbolInfo shouldBe false
    sym.symbolInfo shouldBeTheSameInstanceAs NoSymbolInfo
    sym.name shouldBe Name.NoName
    sym.dataType shouldBe DataType.UnknownType
  }

  test("should return NoSymbolInfo for NoSymbol") {
    Symbol.NoSymbol.symbolInfo shouldBeTheSameInstanceAs NoSymbolInfo
    Symbol.NoSymbol.isNoSymbol shouldBe true
  }

  test("should complete the symbol info lazily and only once") {
    val sym            = Symbol(2, Span.NoSpan)
    var completedCount = 0
    setCompleter(sym, "lazy_val") { s =>
      completedCount += 1
      newSymbolInfo(s, "lazy_val")
    }
    sym.hasSymbolInfo shouldBe true
    completedCount shouldBe 0
    // The name is known eagerly without forcing the completion
    sym.name.name shouldBe "lazy_val"
    completedCount shouldBe 0

    sym.symbolInfo.name.name shouldBe "lazy_val"
    completedCount shouldBe 1
    // The second access must reuse the completed info
    sym.symbolInfo.name.name shouldBe "lazy_val"
    completedCount shouldBe 1
  }

  test("should not force completion from toString") {
    val sym       = Symbol(3, Span.NoSpan)
    var completed = false
    setCompleter(sym, "quiet") { s =>
      completed = true
      newSymbolInfo(s, "quiet")
    }
    sym.toString shouldBe "quiet"
    completed shouldBe false
  }

  test("should let a direct assignment override a pending completer") {
    val sym       = Symbol(4, Span.NoSpan)
    var completed = false
    setCompleter(sym, "stale") { s =>
      completed = true
      newSymbolInfo(s, "stale")
    }
    sym.symbolInfo = newSymbolInfo(sym, "fresh")
    sym.symbolInfo.name.name shouldBe "fresh"
    sym.name.name shouldBe "fresh"
    completed shouldBe false
  }

  test("should let a completer override a previously assigned info (REPL redefinition)") {
    val sym = Symbol(5, Span.NoSpan)
    sym.symbolInfo = newSymbolInfo(sym, "old")
    setCompleter(sym, "new") { s =>
      newSymbolInfo(s, "new")
    }
    sym.symbolInfo.name.name shouldBe "new"
  }

  test("should detect cyclic symbol completion") {
    val sym = Symbol(6, Span.NoSpan)
    setCompleter(sym, "cyclic") { s =>
      // A completer that (indirectly) asks for its own symbol info
      s.symbolInfo
    }
    val e = intercept[WvletLangException] {
      sym.symbolInfo
    }
    e.statusCode shouldBe StatusCode.CYCLIC_SYMBOL_REFERENCE
  }

  test("should report isCompleting while the completer is running") {
    val sym = Symbol(7, Span.NoSpan)
    setCompleter(sym, "probe") { s =>
      s.isCompleting shouldBe true
      newSymbolInfo(s, "probe")
    }
    sym.isCompleting shouldBe false
    sym.symbolInfo.name.name shouldBe "probe"
    sym.isCompleting shouldBe false
  }

  test("should recover after a failed completion attempt") {
    val sym     = Symbol(8, Span.NoSpan)
    var attempt = 0
    setCompleter(sym, "flaky") { s =>
      attempt += 1
      if attempt == 1 then
        throw IllegalStateException("transient failure")
      newSymbolInfo(s, "recovered")
    }
    intercept[IllegalStateException] {
      sym.symbolInfo
    }
    // The completer remains installed, so a later access can complete successfully
    sym.symbolInfo.name.name shouldBe "recovered"
  }

end SymbolCompleterTest

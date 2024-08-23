package wvlet.lang.compiler

import wvlet.airspec.AirSpec

class ScopeTest extends AirSpec:

  test("NoScope") {
    val scope = Scope.NoScope
    scope.isNoScope shouldBe true
    scope.size shouldBe 0
    scope.getAllEntries shouldBe empty
    scope.lookupEntry(Name.termName("x")) shouldBe None

    scope shouldBe Scope.NoScope
    scope shouldNotBe scope.newChildScope
    scope shouldBeTheSameInstanceAs Scope.NoScope
  }

  test("create a new scope") {
    val scope = Scope.NoScope.newChildScope
    scope.nestingLevel shouldBe 1

    val x = Name.termName("x")
    scope.lookupEntry(x) shouldBe None

    val sym    = Symbol(1)
    val retSym = scope.add(x, sym)
    sym shouldBe retSym

    scope.lookupEntry(x) shouldBe Some(ScopeEntry(x, sym, scope))

    scope.filter(_.id == 1) shouldBe List(sym)

    test("child scope") {
      val childScope = scope.newChildScope
      childScope.nestingLevel shouldBe 2
      childScope.lookupEntry(x) shouldBe Some(ScopeEntry(x, sym, scope))
      val sym2 = Symbol(2)
      childScope.add(Name.termName("y"), sym2)

      val localSyms = childScope.getLocalSymbols
      localSyms.size shouldBe 1
      localSyms.head shouldBe sym2

      test("do not select parent symbolx in filter") {
        childScope.filter(_.id == 1) shouldBe empty
      }
    }
  }

end ScopeTest

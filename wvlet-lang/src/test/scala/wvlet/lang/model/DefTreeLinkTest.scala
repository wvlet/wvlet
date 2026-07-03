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
package wvlet.lang.model

import wvlet.uni.test.UniTest
import wvlet.lang.api.Span
import wvlet.lang.compiler.Symbol
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.plan.EmptyRelation
import wvlet.lang.model.plan.Query

/**
  * The symbol-to-definition-tree link must follow a node when the node is replaced (copied) during
  * plan rewriting, and must NOT move when a node that merely carries the symbol as a reference is
  * copied. This invariant is maintained by the copy primitives (copyInstance/copyMetadataFrom), so
  * rewrite phases do not need to re-link definitions manually.
  */
class DefTreeLinkTest extends UniTest:

  private def newQuery: Query = Query(EmptyRelation(Span.NoSpan), Span.NoSpan)

  test("re-point the definition tree when the defining node is copied") {
    val q   = newQuery
    val sym = Symbol(1, Span.NoSpan)
    q.symbol = sym
    sym.tree = q

    val copied = q.copy()
    copied.copyMetadataFrom(q)

    copied.symbol shouldBeTheSameInstanceAs sym
    sym.tree shouldBeTheSameInstanceAs copied
  }

  test("keep the definition tree when a node referencing the symbol is copied") {
    val definition = newQuery
    val sym        = Symbol(2, Span.NoSpan)
    definition.symbol = sym
    sym.tree = definition

    // Another node carrying the same symbol as a reference (not the definition)
    val reference = newQuery
    reference.symbol = sym

    val copiedRef = reference.copy()
    copiedRef.copyMetadataFrom(reference)

    copiedRef.symbol shouldBeTheSameInstanceAs sym
    // The definition link must still point to the original definition
    sym.tree shouldBeTheSameInstanceAs definition
  }

  test("keep NoSymbol untouched when copying an unlabeled node") {
    val q      = newQuery
    val copied = q.copy()
    copied.copyMetadataFrom(q)
    copied.symbol shouldBeTheSameInstanceAs Symbol.NoSymbol
    Symbol.NoSymbol.tree shouldBeTheSameInstanceAs wvlet.lang.model.plan.LogicalPlan.empty
  }

end DefTreeLinkTest

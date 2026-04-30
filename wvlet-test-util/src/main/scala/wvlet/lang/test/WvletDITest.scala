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
package wvlet.lang.test

import wvlet.uni.design.Design
import wvlet.uni.design.Session
import wvlet.uni.io.FileSystemInit
import wvlet.uni.surface.Surface
import wvlet.uni.test.UniTest

/**
  * Drop-in replacement for AirSpec when only `initDesign` + per-test parameter injection + `inCI`
  * are needed. Bridges to uni's [[UniTest]] — start there for the test framework basics.
  *
  * Usage matches AirSpec:
  * {{{
  * class FooTest extends WvletDITest:
  *   initDesign:
  *     _.bindSingleton[FooConnector]
  *
  *   test("read something"): (foo: FooConnector) =>
  *     foo.read() shouldBe …
  * }}}
  *
  * One [[Session]] is built lazily from the accumulated design, started in `beforeAll`, and shut
  * down in `afterAll`.
  */
trait WvletDITest extends UniTest:
  // Force class-loading of FileSystemInit so that uni.io.IO file ops work without an explicit
  // dependency on wvlet.uni.control.IO. Tests that read files (e.g. Profile.getProfile) would
  // otherwise hit `IllegalStateException: FileSystem not initialized` if no other code path
  // had touched control.IO first.
  FileSystemInit.init()

  // Re-export matcher objects so that subclasses can use `shouldBe defined` / `shouldBe empty`
  // without importing them — matches AirSpec's ergonomics.
  export wvlet.uni.test.defined
  export wvlet.uni.test.empty

  private var designTransform: Design => Design = identity

  /**
    * Register a transform that is applied to the design before tests run. Multiple calls compose
    * left-to-right, like AirSpec's `initDesign`.
    */
  protected def initDesign(f: Design => Design): Unit =
    val prev = designTransform
    designTransform = (d: Design) => f(prev(d))

  protected lazy val testDesign: Design = designTransform(Design.newSilentDesign)

  // One session per spec. Singletons and bindInstance values live for the whole spec and are
  // shared across `test(...)` calls — matches AirSpec's default behaviour where, e.g., a
  // `TestTrinoServer` started via `bindInstance` is reachable from every test in the class.
  protected lazy val testSession: Session = testDesign.newSession

  override protected def beforeAll: Unit =
    testSession.start
    super.beforeAll

  override protected def afterAll: Unit =
    try super.afterAll
    finally testSession.shutdown

  /**
    * `true` when running on CI (a non-empty `CI` env var, set by GitHub Actions and most others).
    */
  protected def inCI: Boolean = sys.env.get("CI").exists(_.nonEmpty)

  /**
    * Resolve a dependency from the test design. Equivalent to AirSpec's
    * `test(name) { (d: D) => ... }` when used inside a test body — the only difference is one extra
    * line at the top of the body.
    *
    * {{{
    * test("Create an in-memory schema"):
    *   val duckdb = dep[DuckDBConnector]
    *   duckdb.withConnection { ... }
    * }}}
    *
    * Why not an overloaded `test[D1]`? Scala 3 overload resolution gets confused when a test body
    * contains `shouldBe` (which uses an inline `using TestSource`) — the body is inferred as
    * `(TestSource) ?=> Any`, which then matches `D1 => Any` with `D1 = TestSource` instead of the
    * inherited by-name `test`. A separate getter side-steps the issue.
    */
  inline protected def dep[A]: A = testSession.get[A](Surface.of[A])

end WvletDITest

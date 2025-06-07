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

import wvlet.lang.catalog.{Catalog, InMemoryCatalog}

/**
  * Scala.js implementation that doesn't support static catalog loading
  */
trait FileIOCompatImpl extends FileIOCompat:
  override def loadStaticCatalog(compilerOptions: CompilerOptions): Catalog =
    // Static catalog loading is not supported in Scala.js
    InMemoryCatalog(catalogName = compilerOptions.catalog.getOrElse("memory"), functions = Nil)

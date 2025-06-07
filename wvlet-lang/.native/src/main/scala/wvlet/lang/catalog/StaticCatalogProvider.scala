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
package wvlet.lang.catalog

import wvlet.lang.compiler.DBType
import wvlet.log.LogSupport

/**
  * Scala Native implementation of StaticCatalogProvider that doesn't support file I/O
  */
trait StaticCatalogProvider:
  def loadCatalog(catalogName: String, dbType: DBType, basePath: Any): Option[StaticCatalog]
  def listAvailableCatalogs(basePath: Any): List[(String, DBType)]

object StaticCatalogProvider extends StaticCatalogProvider with LogSupport:

  override def loadCatalog(
      catalogName: String,
      dbType: DBType,
      basePath: Any
  ): Option[StaticCatalog] =
    warn("Static catalog loading is not supported in Scala Native")
    None

  override def listAvailableCatalogs(basePath: Any): List[(String, DBType)] =
    warn("Static catalog listing is not supported in Scala Native")
    List.empty

end StaticCatalogProvider

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
package wvlet.lang.runner.activity

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.WorkEnv

import java.nio.file.Path

/** Factory of the [[ItemStore]] backing a work environment's task queue */
object ItemStores:
  val STORE_TYPE_SQLITE = "sqlite"
  val STORE_TYPE_MEMORY = "memory"

  /** The environment variable overriding the default item store type */
  val STORE_TYPE_ENV = "WVLET_ITEM_STORE"

  /**
    * Create the item store of the given work environment: a SQLite database next to the flow run
    * store by default, so queued and in-flight activity attempts survive a restart. Set
    * WVLET_ITEM_STORE=memory to keep them in memory only
    */
  def forWorkEnv(workEnv: WorkEnv): ItemStore = ofType(
    sys.env.getOrElse(STORE_TYPE_ENV, STORE_TYPE_SQLITE),
    workEnv
  )

  /** Create an item store of the given type (sqlite or memory) under the work environment */
  def ofType(storeType: String, workEnv: WorkEnv): ItemStore =
    storeType match
      case STORE_TYPE_SQLITE =>
        SQLiteItemStore(Path.of(workEnv.targetFolder, "flow-runs", "items.db"))
      case STORE_TYPE_MEMORY =>
        InMemoryItemStore()
      case other =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Unknown item store type: ${other}. Use sqlite or memory")

end ItemStores

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
package wvlet.lang.model.expr

import scala.collection.mutable

object AttributeIndex:
  /**
    * Returns an empty [[AttributeIndex]]
    */
  val empty = AttributeIndex(mutable.LinkedHashSet.empty[AttributeRef])

  /** Constructs a new [[AttributeIndex]] that contains a single [[Attribute]]. */
  def fromAttribute(a: Attribute): AttributeIndex =
    val attrSet = new mutable.LinkedHashSet[AttributeRef]
    attrSet += AttributeRef(a)()
    new AttributeIndex(attrSet)

/**
  * An index structure for holding AttributeRef objects (references to Attribute objects)
  * @param attrSet
  */
class AttributeIndex(attrSet: mutable.LinkedHashSet[AttributeRef])

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
package wvlet.lang.engine.exec

/**
  * Options of a hash join's flat table entries, kept out of Operators.scala, which sits at the size
  * where any growth makes scalafmt rewrap unrelated code
  */
extension (j: HashJoinOp)
  /**
    * Whether the narrow entries of the join's flat table give the row only the bits the build rows
    * need (`narrow_join_entries`); set before the build is made
    */
  def withNarrowEntries(enabled: Boolean): HashJoinOp =
    j.joinBuild.narrowEntries = enabled
    j

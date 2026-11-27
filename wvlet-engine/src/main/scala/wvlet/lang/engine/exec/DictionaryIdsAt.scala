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

import wvlet.lang.engine.vector.*

/**
  * [[GroupTable.groupIdsAt]] through dictionary codes in one pass over the rows
  * (`agg_scratch_buffers`): each row combines its keys' codes and takes the combination's group at
  * once. The column-by-column form zero-fills a combination array, combines the codes into it one
  * key column at a time, and then reads it again to look the groups up, writing into a new id array
  * per batch. Here the ids go into an array the table reuses across batches, which holds because
  * aggregations read group ids only while folding the batch.
  *
  * TPC-H q1's two one-character keys spent about 5% of its CPU in the column-by-column form.
  */
private[exec] object DictionaryIdsAt:

  /**
    * The group ids of the rows at `sel` (row i is row `sel(i)` of the keys, none of them -1), or
    * null unless every key is a string with codes and the combinations number at most half the rows
    */
  def ids(table: GroupTable, norm: Array[Vec], n: Int, sel: Array[Int]): Array[Int] =
    val width = norm.length
    val m     = table.codeCombinations(norm, n)
    if m < 0 || width > 2 then
      return null
    val groups = table.codeGroupsFor(m)
    val ids    = table.idsFor(n)
    val c0     = norm(0).asInstanceOf[StrVec].codes
    var i      = 0
    if width == 1 then
      while i < n do
        val r = sel(i)
        val x = c0(r)
        ids(i) =
          if x < 0 then
            table.groupOfRow(norm, r)
          else
            lookup(table, groups, x, norm, r)
        i += 1
    else
      val v1   = norm(1).asInstanceOf[StrVec]
      val c1   = v1.codes
      val size = v1.dictSize
      while i < n do
        val r = sel(i)
        val a = c0(r)
        val b = c1(r)
        ids(i) =
          if (a | b) < 0 then
            table.groupOfRow(norm, r)
          else
            lookup(table, groups, a * size + b, norm, r)
        i += 1
    ids

  end ids

  /** The group of combination x, first found or inserted through row r */
  private inline def lookup(
      table: GroupTable,
      groups: Array[Int],
      x: Int,
      norm: Array[Vec],
      r: Int
  ): Int =
    val g = groups(x)
    if g >= 0 then
      g
    else
      val ng = table.groupOfRow(norm, r)
      groups(x) = ng
      ng

end DictionaryIdsAt

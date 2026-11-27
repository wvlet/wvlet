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
  * The string codes of a join build (`join_build_string_codes`): probe outputs gathering them carry
  * codes shared by every batch, for aggregations grouping by them
  */
object JoinBuildCodes:

  /**
    * The build (the concatenation of `batches`) with its string columns encoded over their distinct
    * values ([[StrVec.encodeParts]]), when it has at most [[JoinBuild.MaxCodedRows]] rows: probe
    * outputs gather the codes, which then identify values across batches. Each dictionary entry of
    * a batch carrying Parquet dictionary codes (`dictionary_group_keys` scans) is hashed once; up
    * to [[JoinBuild.MaxPlainCodedRows]] rows, rows without codes are hashed one by one.
    *
    * All or nothing: the build keeps the codes only when every string column encodes (repeating
    * values: at most half the rows distinct), since an aggregation takes the codes only when all
    * its string keys carry them, and builds hold only the columns their joins use. Otherwise probe
    * outputs would gather codes no aggregation reads. A large build with a column that a Parquet
    * writer left PLAIN (ids, names) is left as it is without encoding any column. Columns already
    * carrying a `dict` stay as they are.
    *
    * With `probe` (`string_code_probe`), a column gives up after a few rows when they look nearly
    * unique ([[StrVec.encodeParts]]), and the columns after the first that gives up are not
    * encoded: TPC-H q2's supplier build hashed 10K rows of each of `s_name`, `s_address`, `s_phone`
    * and `s_comment`, all unique, on one thread before keeping none of their codes
    */
  def encodeStrings(
      build: Batch,
      batches: Seq[Batch],
      steps: TaskRunner = TaskRunner.Sequential,
      probe: Boolean = false
  ): Batch =
    if build.size == 0 || build.size > JoinBuild.MaxCodedRows then
      build
    else
      val cols = build.columns
      val out  = cols.clone()
      val strs = cols
        .indices
        .filter { k =>
          cols(k) match
            case v: StrVec =>
              v.dict == null
            case _ =>
              false
        }
      val hashPlain = build.size <= JoinBuild.MaxPlainCodedRows
      // A large build with a string column without Parquet codes keeps no codes at all
      def plain(k: Int): Boolean = batches.exists { b =>
        b.column(k) match
          case p: StrVec =>
            !p.hasCodes
          case _ =>
            true
      }
      if strs.isEmpty || (!hashPlain && strs.exists(plain)) then
        return build
      // One task per string column, on the build runner for large builds
      // Set once a column keeps no codes: then no column does, so the others needn't be encoded
      val failed = java.util.concurrent.atomic.AtomicBoolean(false)
      JoinBuildSteps.runAll(strs.size, steps) { j =>
        val k = strs(j)
        if !(probe && failed.get()) then
          out(k) = StrVec.encodeParts(
            cols(k).asInstanceOf[StrVec],
            batches.map(_.column(k)),
            math.max(1, build.size / 2),
            hashPlain,
            probe
          )
          if out(k) eq cols(k) then
            failed.set(true)
      }
      if strs.forall(k => out(k) ne cols(k)) then
        Batch(out, build.size)
      else
        build

end JoinBuildCodes

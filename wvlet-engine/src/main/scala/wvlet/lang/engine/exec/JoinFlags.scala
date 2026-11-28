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
  * Copying a hash join's flags to the join that rebuilds it. The flags are mutable fields of
  * [[HashJoinOp]] and its [[JoinBuild]], set by the planner after lowering; a rebuild that sets
  * them one by one can miss a new flag, so every rebuild site calls [[withFlagsOf]], and a new flag
  * is added here (`EngineJoinFlagsTest` checks that each one survives)
  */
extension (j: HashJoinOp)
  /** Take the flags of `o`, the join this one rebuilds; the prefilters are bound separately */
  def withFlagsOf(o: HashJoinOp): HashJoinOp = j
    .withLateGather(o.lateGather)
    .withDirectIndex(o.directIndex)
    .withParallelBuild(o.parallelBuild)
    .withStringCodes(o.stringCodes)
    .withCodeProbe(o.joinBuild.codeProbe)
    .withUnmatchedOnly(o.unmatchedOnly)
    .withMissFilter(o.joinBuild.missFilter)
    .withNarrowEntries(o.joinBuild.narrowEntries)
    .withParallelKeys(o.joinBuild.parallelKeys)
    .withSpill(o.spills)

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
package wvlet.lang.engine

import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.PlannerMode
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.jdk.CollectionConverters.*

/**
  * The engine's physical plans of the TPC-H and TPC-DS queries must match the golden files under
  * spec/engine-plans, so that a planner change shows its plan changes in the PR diff. Regenerate
  * them with [[EnginePlanGolden.regenCommand]]. The files hold the runtime planner's plans. The
  * recursive (legacy) planner is checked against the same files, except where its plan differs:
  * those plans are under spec/engine-plans/legacy, so that a rule only the runtime planner has
  * (e.g., PushDownFilters) shows which plans it changes
  */
class EnginePlanGoldenTest extends UniTest:
  import EnginePlanGolden.*

  for
    suite   <- suites
    planner <- PlannerMode.values
  do
    test(s"${suite.name} plans match spec/engine-plans/${suite.name} (${planner.name} planner)") {
      if TaskRunner.default.parallelism <= 1 then
        ignore("The golden plans are parallel plans: run on a machine with 2 or more processors")
      val generated = suite.generate(
        OptimizerOptions().withPlanner(planner).withValidatePlans(planner == PlannerMode.Runtime)
      )
      val dir      = suite.dirOf(planner)
      val existing =
        if Files.isDirectory(dir) then
          planFiles(dir).map(_.getFileName.toString).toSet
        else
          Set.empty[String]
      val drift = Seq.newBuilder[String]
      generated.foreach { (file, plan) =>
        val path = suite.goldenFile(planner, file)
        if !Files.exists(path) then
          drift += s"missing: ${path}"
        else
          val golden = String(Files.readAllBytes(path))
          if golden != plan then
            val g    = golden.linesIterator.toIndexedSeq
            val p    = plan.linesIterator.toIndexedSeq
            val line = g.zipAll(p, "<missing line>", "<missing line>").indexWhere(_ != _)
            drift += s"""changed: ${path} (line ${line + 1})
                 |  golden   : ${g.lift(line).getOrElse("<missing line>")}
                 |  generated: ${p.lift(line).getOrElse("<missing line>")}""".stripMargin
      }
      (existing -- generated.keySet).foreach(f => drift += s"stale: ${dir.resolve(f)}")
      if planner == PlannerMode.Legacy then
        // A legacy plan that matches the runtime planner's is not kept separately
        existing.foreach { f =>
          val legacy = dir.resolve(f)
          val shared = suite.dir.resolve(f)
          if Files.exists(shared) &&
            Files.readAllBytes(legacy).sameElements(Files.readAllBytes(shared))
          then
            drift += s"stale: ${legacy}"
        }
      val d = drift.result()
      if d.nonEmpty then
        fail(s"""Engine plans changed for ${d.size} ${suite.name} file(s):
             |${d.take(10).mkString("\n")}
             |If the plan changes are intended, regenerate the golden files with:
             |  ${regenCommand}""".stripMargin)
    }

  end for

end EnginePlanGoldenTest

/** Writes the golden plan files: `./sbt "engineJVM/Test/runMain ..."` (see [[regenCommand]]) */
object EnginePlanGolden:
  val regenCommand: String = """./sbt "engineJVM/Test/runMain wvlet.lang.engine.EnginePlanGolden""""

  val root: Path = Paths.get("spec/engine-plans")

  /** A query set and the data it plans over */
  case class Suite(name: String, specPath: String, dataDir: () => (String, Seq[String])):
    def dir: Path = root.resolve(name)

    /** The folder of the plans of a planner that differ from the runtime planner's */
    def dirOf(planner: PlannerMode): Path =
      planner match
        case PlannerMode.Runtime =>
          dir
        case PlannerMode.Legacy =>
          root.resolve("legacy").resolve(name)

    /** The golden file of a plan of a planner: its own, or the runtime planner's */
    def goldenFile(planner: PlannerMode, file: String): Path =
      val own = dirOf(planner).resolve(file)
      if planner == PlannerMode.Runtime || Files.exists(own) then
        own
      else
        dir.resolve(file)

    /** Plan file name -> plan text, for every unit of the spec folder */
    def generate(optimizer: OptimizerOptions = OptimizerOptions()): Map[String, String] =
      val (data, tables) = dataDir()
      val c              = EngineConformance(
        specPath,
        Nil,
        engine => tables.foreach(t => engine.catalog.registerParquet(t, s"${data}/${t}.parquet")),
        optimizer
      )
      try
        c.units
          .map { unit =>
            val name  = unit.sourceFile.fileName.stripSuffix(".wv")
            val plans = c.explain(unit)
            val text  = plans
              .zipWithIndex
              .map { (p, i) =>
                s"-- ${name}#${i + 1}\n${normalize(p, data)}\n"
              }
              .mkString("\n")
            s"${name}.plan" -> text
          }
          .filter(_._2.nonEmpty)
          .toMap
      finally c.close()

  end Suite

  val suites: Seq[Suite] = Seq(
    Suite("tpch", "spec/tpch", () => ("spec/tpch/data", EngineTPCHBench.tables)),
    Suite("tpcds", "spec/tpcds", () => EngineTPCDSTest.prepareData(EngineTPCDSTest.scaleFactor))
  )

  /** The golden files of a suite folder (other files there are neither checked nor deleted) */
  def planFiles(dir: Path): Seq[Path] =
    Files.list(dir).iterator().asScala.filter(_.getFileName.toString.endsWith(".plan")).toSeq

  /** Remove what depends on the machine: the data folder and the thread count */
  private def normalize(plan: String, dataDir: String): String = plan
    .replace(Paths.get(dataDir).toAbsolutePath.toString, "<data>")
    .replace(dataDir, "<data>")
    .replaceAll("parallelism=\\d+", "parallelism=N")

  def main(args: Array[String]): Unit = suites.foreach { suite =>
    val generated = suite.generate(OptimizerOptions().withPlanner(PlannerMode.Runtime))
    Files.createDirectories(suite.dir)
    planFiles(suite.dir)
      .filterNot(p => generated.contains(p.getFileName.toString))
      .foreach(Files.delete)
    generated.foreach((file, plan) => Files.write(suite.dir.resolve(file), plan.getBytes))
    println(s"Wrote ${generated.size} plans to ${suite.dir}")
    // The legacy planner's plans, where they differ
    val legacyDir = suite.dirOf(PlannerMode.Legacy)
    if Files.isDirectory(legacyDir) then
      planFiles(legacyDir).foreach(Files.delete)
    val differing = suite
      .generate(OptimizerOptions().withPlanner(PlannerMode.Legacy))
      .filter((file, plan) => !generated.get(file).contains(plan))
    if differing.nonEmpty then
      Files.createDirectories(legacyDir)
      differing.foreach((file, plan) => Files.write(legacyDir.resolve(file), plan.getBytes))
    else if Files.isDirectory(legacyDir) then
      Files.delete(legacyDir)
    println(s"Wrote ${differing.size} legacy plans to ${legacyDir}")
  }

end EnginePlanGolden

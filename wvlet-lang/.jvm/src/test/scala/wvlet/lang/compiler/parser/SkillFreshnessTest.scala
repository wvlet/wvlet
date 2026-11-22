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
package wvlet.lang.compiler.parser

import wvlet.uni.test.UniTest

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters.*

/**
  * Fails when the language gains syntax that the bundled AI-agent skill does not teach yet.
  *
  *   - The reserved-keyword list in the skill must match [[WvletToken]].
  *   - Every keyword must appear in code (a fence or an inline code span) somewhere in the skill,
  *     unless it is listed in [[notInSkill]] with a reason.
  */
class SkillFreshnessTest extends UniTest:
  import SkillFreshnessTest.*

  private val skillFiles: List[File] =
    Files
      .walk(File(SkillKeywordGenerator.skillDir).toPath)
      .iterator()
      .asScala
      .map(_.toFile)
      .filter(f => f.isFile && f.getName.endsWith(".md"))
      .toList

  private def read(f: File): String = Files.readString(f.toPath)

  test("reserved keyword list in the skill matches the parser") {
    val text = Files.readString(File(SkillKeywordGenerator.targetPath).toPath)
    if SkillKeywordGenerator.updated(text) != text then
      fail(s"""The reserved keyword list in ${SkillKeywordGenerator.targetPath} is out of date.
           |Regenerate it with:
           |  ${SkillKeywordGenerator.regenCommand}""".stripMargin)
  }

  test("every keyword is taught by the skill") {
    // The generated keyword list names every keyword, so it does not count as teaching one
    val code = skillFiles
      .map(f => codeOf(SkillKeywordGenerator.withoutGeneratedBlock(read(f))))
      .mkString("\n")
    val missing = WvletToken
      .keywords
      .map(_.str)
      .distinct
      .filterNot(notInSkill.contains)
      .filterNot { k =>
        s"(?<![A-Za-z0-9_])${java.util.regex.Pattern.quote(k)}(?![A-Za-z0-9_])"
          .r
          .findFirstIn(code)
          .isDefined
      }
    if missing.nonEmpty then
      fail(
        s"""The bundled skill (${SkillKeywordGenerator
            .skillDir}) does not show these keywords in any code example:
           |  ${missing.mkString(", ")}
           |Add an example that compiles (see AGENTS.md, "Bundled Skill"), or list the keyword in
           |SkillFreshnessTest.notInSkill with the reason agents do not need it.""".stripMargin
      )
  }

  test("keywords excused from the skill still exist") {
    val keywords = WvletToken.keywords.map(_.str).toSet
    notInSkill.keySet.filterNot(keywords.contains) shouldBe Set.empty
  }

end SkillFreshnessTest

object SkillFreshnessTest:
  private val anyFence   = """(?s)```.*?```""".r
  private val wvletFence = """(?s)```wvlet\b.*?```""".r
  private val inlineCode = """`[^`\n]+`""".r

  /**
    * Text of `wvlet` code fences and inline code spans, where syntax examples live. Other fences
    * (JSON, shell) are excluded so that, e.g., `"type": "trino"` does not count as teaching `type`.
    */
  def codeOf(markdown: String): String =
    val fences = wvletFence.findAllIn(markdown).toList
    val rest   = anyFence.replaceAllIn(markdown, "")
    (fences ++ inlineCode.findAllIn(rest)).mkString("\n")

  /**
    * Keywords the skill deliberately does not teach, with the reason
    */
  val notInSkill: Map[String, String] = Map(
    "inline"   -> "internal modifier for stdlib function definitions",
    "native"   -> "internal modifier for stdlib function definitions",
    "this"     -> "used only inside trait method bodies of the stdlib",
    "package"  -> "module declarations; agents write single-folder projects",
    "export"   -> "module re-exports; not needed for queries",
    "execute"  -> "flow pre/post hooks; see the flow docs",
    "run"      -> "runs another .wv file from a script; rarely needed by agents",
    "let"      -> "reserved for future use",
    "of"       -> "used only in internal type syntax",
    "route"    -> "flow operator; see the flow docs",
    "fork"     -> "flow operator; see the flow docs",
    "merge"    -> "flow operator; see the flow docs",
    "depends"  -> "flow dependency; see the flow docs",
    "wait"     -> "flow operator; see the flow docs",
    "activate" -> "flow operator; see the flow docs",
    "activity" -> "flow activity stages; see the flow docs",
    "each"     -> "flow fan-out; see the flow docs",
    "end"      -> "flow termination; agents must not write `end` after case expressions",
    "call"     -> "connector tool invocation; see the connector docs"
  )

end SkillFreshnessTest

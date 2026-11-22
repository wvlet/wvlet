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
package wvlet.lang.skill

import wvlet.lang.api.WvletLangException
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

class SkillTest extends UniTest:
  private val skillDir = Path.of("plugins/wvlet/skills/wvlet")

  test("embed every file of the skill folder verbatim") {
    val onDisk =
      Files
        .walk(skillDir)
        .filter(p => Files.isRegularFile(p) && p.toString.endsWith(".md"))
        .map(p => skillDir.relativize(p).toString.replace('\\', '/'))
        .toArray
        .map(_.toString)
        .toSet
    Skill.fileNames.toSet shouldBe onDisk
    for name <- Skill.fileNames do
      Skill.read(name) shouldBe Files.readString(skillDir.resolve(name))
  }

  test("find a reference by path, file name, or base name") {
    val syntax = Skill.read("references/syntax.md")
    Skill.read("syntax.md") shouldBe syntax
    Skill.read("syntax") shouldBe syntax
    Skill.read("SKILL.md") shouldContain "name: wvlet"
  }

  test("report the available files for an unknown name") {
    val e = intercept[WvletLangException] {
      Skill.read("no-such-file")
    }
    e.getMessage shouldContain "references/syntax.md"
  }

  test("install the skill into a folder") {
    val dir     = Files.createTempDirectory(Path.of("target").toAbsolutePath, "skill-install")
    val written = Skill.install(dir.toString)
    written.size shouldBe Skill.fileNames.size
    Files.readString(dir.resolve("wvlet/SKILL.md")) shouldBe Skill.read("SKILL.md")
    // Installing again overwrites the files
    Skill.install(dir.toString).size shouldBe written.size
  }

end SkillTest

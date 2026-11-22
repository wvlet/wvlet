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
import wvlet.uni.io.IOPath
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Path

class SkillInstallerTest extends UniTest:
  private def tempDir(prefix: String): Path = Files.createTempDirectory(
    Files.createDirectories(Path.of("target").toAbsolutePath),
    prefix
  )

  private def newInstaller(version: String = "1.0"): (SkillInstaller, Path, Path) =
    val project   = tempDir("skill-project")
    val home      = tempDir("skill-home")
    val installer = SkillInstaller(
      version,
      baseDir = IOPath.parse(project.toString),
      homeDir = IOPath.parse(home.toString),
      claudeConfigDir = None
    )
    (installer, project, home)

  private def statuses(results: Seq[SkillResult]): Seq[String] = results.map(_.status)

  test("install for every agent into the project folders") {
    val (installer, project, _) = newInstaller()
    val results                 = installer.install(Nil)
    statuses(results) shouldBe Seq("installed", "installed")
    results.map(_.agents) shouldContain Seq("codex", "pi")
    results.map(_.agents) shouldContain Seq("claude")
    Files.readString(project.resolve(".claude/skills/wvlet/SKILL.md")) shouldBe
      Skill.read("SKILL.md")
    Files.exists(project.resolve(".agents/skills/wvlet/references/syntax.md")) shouldBe true
  }

  test("install only for the named agent, in the home folder with --global") {
    val (installer, project, home) = newInstaller()
    statuses(installer.install(SkillAgent.parse(Seq("claude-code")), global = true)) shouldBe
      Seq("installed")
    Files.exists(home.resolve(".claude/skills/wvlet/SKILL.md")) shouldBe true
    Files.exists(home.resolve(".agents")) shouldBe false
    Files.exists(project.resolve(".claude")) shouldBe false
  }

  test("use CLAUDE_CONFIG_DIR for a global Claude Code install") {
    val config    = tempDir("claude-config")
    val installer = SkillInstaller(
      "1.0",
      homeDir = IOPath.parse(tempDir("skill-home").toString),
      claudeConfigDir = Some(IOPath.parse(config.toString))
    )
    installer.install(Seq(SkillAgent.Claude), global = true)
    Files.exists(config.resolve("skills/wvlet/SKILL.md")) shouldBe true
  }

  test("report unchanged on reinstall and update an older install") {
    val (installer, project, _) = newInstaller()
    installer.install(Seq(SkillAgent.Codex))
    val again = installer.install(Seq(SkillAgent.Codex))
    statuses(again) shouldBe Seq("unchanged")
    // Codex and Pi share the folder, so both are reported
    again.head.agents shouldBe Seq("codex", "pi")

    // Simulate an install by an older version that shipped a different file set
    val root   = project.resolve(".agents/skills/wvlet")
    val legacy = root.resolve("references/legacy.md")
    Files.writeString(legacy, "old")
    val marker = SkillMarker("0.9", "stale", Skill.fileNames :+ "references/legacy.md")
    Files.writeString(
      root.resolve(SkillInstaller.markerFile),
      wvlet.uni.weaver.Weaver.of[SkillMarker].toJson(marker)
    )
    // The marker hash no longer matches, so the folder counts as edited
    statuses(installer.install(Seq(SkillAgent.Codex))) shouldBe Seq("skipped")
    statuses(installer.install(Seq(SkillAgent.Codex), force = true)) shouldBe Seq("updated")
    Files.exists(legacy) shouldBe false
    installer.list().map(_.status) shouldBe Seq("current")
  }

  test("keep folders the user created or edited") {
    val (installer, project, _) = newInstaller()
    val own                     = project.resolve(".claude/skills/wvlet")
    Files.createDirectories(own)
    Files.writeString(own.resolve("SKILL.md"), "my notes")
    val skipped = installer.install(Seq(SkillAgent.Claude))
    statuses(skipped) shouldBe Seq("skipped")
    skipped.head.detail.get shouldContain "not installed by wvlet"
    Files.readString(own.resolve("SKILL.md")) shouldBe "my notes"

    installer.install(Seq(SkillAgent.Codex))
    val edited = project.resolve(".agents/skills/wvlet/SKILL.md")
    Files.writeString(edited, "edited")
    statuses(installer.install(Seq(SkillAgent.Codex))) shouldBe Seq("skipped")
    statuses(installer.remove(Seq(SkillAgent.Codex))) shouldBe Seq("skipped")
    Files.readString(edited) shouldBe "edited"
  }

  test("remove only what wvlet installed, and preview with dry run") {
    val (installer, project, _) = newInstaller()
    installer.install(Nil)
    statuses(installer.remove(Seq(SkillAgent.Claude), dryRun = true)) shouldBe Seq("would-remove")
    Files.exists(project.resolve(".claude/skills/wvlet")) shouldBe true
    statuses(installer.remove(Seq(SkillAgent.Claude))) shouldBe Seq("removed")
    Files.exists(project.resolve(".claude/skills/wvlet")) shouldBe false
    Files.exists(project.resolve(".agents/skills/wvlet")) shouldBe true
    statuses(installer.remove(Seq(SkillAgent.Claude))) shouldBe Seq("missing")
  }

  test("install into an exact folder with --dir") {
    val (installer, _, _) = newInstaller()
    val dir               = tempDir("skill-dir")
    statuses(installer.install(Nil, dir = Some(dir.toString))) shouldBe Seq("installed")
    Files.exists(dir.resolve("wvlet/SKILL.md")) shouldBe true
  }

  test("run subcommands and print JSON") {
    val (installer, _, _) = newInstaller()
    SkillOption.run(SkillOption(), installer) shouldContain "name: wvlet"
    SkillOption.run(SkillOption(args = Seq("show", "testing")), installer) shouldBe
      Skill.read("references/testing.md")
    SkillOption.run(SkillOption(args = Seq("files")), installer) shouldContain
      "references/syntax.md"
    val json = SkillOption.run(
      SkillOption(args = Seq("install"), agents = Seq("codex,pi"), json = true),
      installer
    )
    json shouldContain "\"status\":\"installed\""
    intercept[WvletLangException] {
      SkillOption.run(SkillOption(args = Seq("remove")), installer)
    }
    intercept[WvletLangException] {
      SkillAgent.parse(Seq("cursor"))
    }
  }

  test("never delete paths outside the folder that a tampered marker lists") {
    val (installer, project, _) = newInstaller()
    installer.install(Seq(SkillAgent.Codex))
    val outside = project.resolve("precious.txt")
    Files.writeString(outside, "keep")
    val root = project.resolve(".agents/skills/wvlet")
    for bad <- Seq("../../../precious.txt", outside.toString) do
      val marker = SkillMarker("0.9", "x", Skill.fileNames :+ bad)
      Files.writeString(
        root.resolve(SkillInstaller.markerFile),
        wvlet.uni.weaver.Weaver.of[SkillMarker].toJson(marker)
      )
      val result = installer.install(Seq(SkillAgent.Codex))
      statuses(result) shouldBe Seq("skipped")
      result.head.detail.get shouldContain "outside the skill folder"
      installer.install(Seq(SkillAgent.Codex), force = true)
      installer.remove(Seq(SkillAgent.Codex), force = true)
      Files.readString(outside) shouldBe "keep"
      installer.install(Seq(SkillAgent.Codex))
  }

  test("keep files the user added when removing") {
    val (installer, project, _) = newInstaller()
    installer.install(Seq(SkillAgent.Claude))
    val notes = project.resolve(".claude/skills/wvlet/team-notes.md")
    Files.writeString(notes, "ours")
    installer.list().map(_.status) shouldBe Seq("edited")
    statuses(installer.remove(Seq(SkillAgent.Claude))) shouldBe Seq("skipped")
    // --force deletes only what wvlet wrote
    statuses(installer.remove(Seq(SkillAgent.Claude), force = true)) shouldBe Seq("removed")
    Files.readString(notes) shouldBe "ours"
    Files.exists(project.resolve(".claude/skills/wvlet/SKILL.md")) shouldBe false
    Files.exists(project.resolve(".claude/skills/wvlet/references")) shouldBe false
  }

  test("list an install made with --dir") {
    val (installer, _, _) = newInstaller()
    val dir               = tempDir("skill-dir")
    installer.install(Nil, dir = Some(dir.toString))
    installer.list(dir = Some(dir.toString)).map(_.status) shouldBe Seq("current")
    installer.list().isEmpty shouldBe true
  }

end SkillInstallerTest

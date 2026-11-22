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

import java.nio.file.Files
import java.nio.file.Path

/**
  * Keeps the keyword list of the bundled AI-agent skill (plugins/wvlet/skills/wvlet) in sync with
  * [[WvletToken]]. The list sits between marker comments in [[targetPath]]; running this object
  * rewrites it.
  */
object SkillKeywordGenerator:
  val skillDir     = "plugins/wvlet/skills/wvlet"
  val targetPath   = s"${skillDir}/references/syntax.md"
  val regenCommand =
    "./sbt \"langJVM/Test/runMain wvlet.lang.compiler.parser.SkillKeywordGenerator\""

  private val beginMarker = "<!-- BEGIN GENERATED: reserved-keywords -->"
  private val endMarker   = "<!-- END GENERATED: reserved-keywords -->"

  def reservedKeywords: Seq[String] =
    WvletToken.keywords.filter(_.isReservedKeyword).map(_.str).distinct.sorted

  /**
    * The generated block, including the marker lines
    */
  def generatedBlock: String =
    val list = reservedKeywords.map(k => s"`${k}`").mkString(", ")
    s"""${beginMarker}
       |${list}
       |${endMarker}""".stripMargin

  /**
    * Returns the given Markdown text with its generated block replaced by the current keyword list
    */
  def updated(markdown: String): String =
    val begin = markdown.indexOf(beginMarker)
    val end   = markdown.indexOf(endMarker)
    require(
      begin >= 0 && end > begin,
      s"${targetPath} must contain the markers ${beginMarker} and ${endMarker}"
    )
    markdown.substring(0, begin) + generatedBlock + markdown.substring(end + endMarker.length)

  /**
    * Returns the given Markdown text without its generated block
    */
  def withoutGeneratedBlock(markdown: String): String =
    val begin = markdown.indexOf(beginMarker)
    val end   = markdown.indexOf(endMarker)
    if begin >= 0 && end > begin then
      markdown.substring(0, begin) + markdown.substring(end + endMarker.length)
    else
      markdown

  def main(args: Array[String]): Unit =
    val path = Path.of(targetPath)
    val text = Files.readString(path)
    Files.writeString(path, updated(text))
    println(s"Updated ${targetPath}")

end SkillKeywordGenerator

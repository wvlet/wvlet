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
package wvlet.lang.catalog

import wvlet.airframe.codec.MessageCodec
import wvlet.lang.compiler.DBType
import wvlet.log.LogSupport
import wvlet.log.io.IOUtil

import java.io.File
import scala.jdk.CollectionConverters.*

case class Profile(
    name: String,
    `type`: String,
    user: Option[String] = Some("user"),
    password: Option[String] = None,
    host: Option[String] = None,
    port: Option[Int] = None,
    catalog: Option[String] = None,
    schema: Option[String] = None,
    properties: Map[String, Any] = Map.empty
):
  def dbType: DBType = DBType.fromString(`type`)

object Profile extends LogSupport:

  def getProfile(
      profile: Option[String],
      catalog: Option[String] = None,
      schema: Option[String] = None
  ): Profile =
    val currentProfile: Profile = profile
      .flatMap { targetProfile =>
        getProfile(targetProfile) match
          case Some(p) =>
            debug(s"Using profile: ${targetProfile}")
            Some(p)
          case None =>
            error(s"No profile ${targetProfile} found")
            None
      }
      .getOrElse {
        // Use default DuckDB profile
        Profile(name = "local", `type` = "duckdb", catalog = Some("memory"), schema = Some("main"))
      }

    currentProfile.copy(
      catalog = catalog.orElse(currentProfile.catalog),
      schema = schema.orElse(currentProfile.schema)
    )

  end getProfile

  def getProfile(profile: String): Option[Profile] =
    val configPath = sys.props("user.home") + "/.wvlet/profiles.yml"
    val configFile = new File(configPath)
    if !configFile.exists() then
      None
    else
      val yamlString = IOUtil.readAsString(configPath)
      // replace environment variables ($xxxx) in the yaml to real env values
      val yamlStringEvaluated = yamlString
        .split("\n")
        .map { line =>
          val envPattern = """\$([A-Za-z0-9_]+)""".r
          envPattern.replaceAllIn(line, m => sys.env.getOrElse(m.group(1), m.matched))
        }
        .mkString("\n")

      val yamlMap =
        new org.yaml.snakeyaml.Yaml()
          .load(yamlStringEvaluated)
          .asInstanceOf[java.util.Map[AnyRef, AnyRef]]
          .asScala
          .toMap
      yamlMap.get("profiles") match
        case Some(profs: java.util.List[?]) =>
          val codec = MessageCodec.of[Profile]
          profs
            .asScala
            .collect { case p: java.util.HashMap[?, ?] =>
              p.asScala
                .map { case (k, v) =>
                  k.toString -> v
                }
                .toMap[String, Any]
            }
            .map { (m: Map[String, Any]) =>
              codec.fromMap(m)
            }
            .find(_.name == profile)
        case _ =>
          None
    end if

  end getProfile

end Profile

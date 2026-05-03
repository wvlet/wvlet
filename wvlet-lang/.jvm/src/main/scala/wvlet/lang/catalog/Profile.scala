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

import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.weaver.Weaver
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.DBType
import wvlet.uni.log.LogSupport
import wvlet.uni.io.IO

import java.io.File

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
  def dbType: DBType                                 = DBType.fromString(`type`)
  def withProperty(key: String, value: Any): Profile = copy(properties =
    properties + (key -> value)
  )

object Profile extends LogSupport:

  // Matches both $VAR and ${VAR} forms anywhere in the file text.
  // Groups: 1 = bare $VAR; 2 = ${VAR}.
  private val envVarPattern = """\$\{([A-Za-z0-9_]+)\}|\$([A-Za-z0-9_]+)""".r

  // Wraps the top-level shape of profiles.json: {"profiles": [Profile, ...]}.
  // Private to keep the on-disk schema an implementation detail.
  private case class ProfileConfig(profiles: Seq[Profile] = Seq.empty)

  def defaultGenericProfile = Profile(name = "local", `type` = "generic")

  def defaultDuckDBProfile = Profile(
    name = "local",
    `type` = "duckdb",
    catalog = Some("memory"),
    schema = Some("main")
  )

  def defaultProfileFor(dbType: DBType): Profile =
    dbType match
      case DBType.DuckDB =>
        defaultDuckDBProfile
      case other =>
        Profile(name = "local", `type` = other.toString.toLowerCase)

  def getProfile(
      profile: Option[String],
      catalog: Option[String] = None,
      schema: Option[String] = None,
      default: => Profile = defaultGenericProfile
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
        // Use the default profile
        default
      }

    currentProfile.copy(
      catalog = catalog.orElse(currentProfile.catalog),
      schema = schema.orElse(currentProfile.schema)
    )

  end getProfile

  def getProfile(profile: String): Option[Profile] =
    val configDir = sys.props("user.home") + "/.wvlet"
    resolveConfigPath(configDir) match
      case Some(configPath) =>
        readProfileConfig(configPath).profiles.find(_.name == profile)
      case None =>
        None

  end getProfile

  // Returns the path of the first profile config file found, or None.
  // Falls back to a deprecation warning when only a YAML file is present.
  private def resolveConfigPath(configDir: String): Option[String] =
    val candidates = Seq("profiles.json", "profiles.jsonc")
    val found = candidates.iterator.map(name => s"${configDir}/${name}").find(p => File(p).exists())
    found.orElse {
      val legacy = Seq("profiles.yml", "profiles.yaml")
        .map(name => s"${configDir}/${name}")
        .find(p => File(p).exists())
      legacy.foreach { path =>
        warn(
          s"YAML profile config at ${path} is no longer read. Convert it to ${configDir}/profiles.json " +
            s"(JSONC: comments and trailing commas allowed). Quick conversion: `yq -o=json ${path} > ${configDir}/profiles.json`."
        )
      }
      None
    }

  private def readProfileConfig(configPath: String): ProfileConfig =
    val rawText  = IO.readString(IO.path(configPath))
    val parsed   = JSON.parse(rawText)
    val expanded = expandEnvVarsInStrings(parsed, configPath)
    Weaver.of[ProfileConfig].fromJSONValue(expanded)

  // Walk the parsed JSON and replace $VAR / ${VAR} occurrences inside string
  // values with values from the process env. Operating on the parsed tree (not
  // the raw text) means JSONC comments like `// uses ${TD_API_KEY}` and
  // metadata keys like `"$schema"` don't trigger env lookup, and the env
  // values bypass JSON escaping entirely (Weaver consumes JSONString directly).
  private def expandEnvVarsInStrings(value: JSONValue, configPath: String): JSONValue =
    value match
      case JSONString(s) =>
        JSONString(interpolate(s, configPath))
      case JSONObject(pairs) =>
        JSONObject(
          pairs.map { case (k, v) =>
            k -> expandEnvVarsInStrings(v, configPath)
          }
        )
      case JSONArray(items) =>
        JSONArray(items.map(expandEnvVarsInStrings(_, configPath)))
      case other =>
        other

  // Substitute $VAR / ${VAR} occurrences in a single string literal.
  // Throws WvletLangException(INVALID_ARGUMENT) for any unresolved variable so
  // typos in config never silently produce a profile pointing at the wrong host.
  private def interpolate(text: String, configPath: String): String = envVarPattern.replaceAllIn(
    text,
    m =>
      val envVar = Option(m.group(1)).getOrElse(m.group(2))
      val value  = sys
        .env
        .getOrElse(
          envVar,
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"Environment variable '${envVar}' is not set but required in profile configuration at ${configPath}"
            )
        )
      // Escape regex backreferences ($, \) so values like passwords with $ chars don't blow up.
      java.util.regex.Matcher.quoteReplacement(value)
  )

end Profile

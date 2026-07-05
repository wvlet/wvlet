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
import wvlet.uni.weaver.MapWeaver
import wvlet.uni.weaver.SeqWeaver
import wvlet.uni.weaver.Weaver
import wvlet.uni.weaver.codec.AnyWeaver
import wvlet.uni.weaver.codec.PrimitiveWeaver.given
import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.DBType
import wvlet.uni.log.LogSupport
import wvlet.uni.io.IO

/**
  * A single named connection inside a [[Profile]]. The `name` is user-chosen and referenced from
  * queries (e.g., `from td.table`); `type` selects the connector implementation ("duckdb", "trino",
  * "snowflake", ...).
  */
case class ConnectorConfig(
    name: String,
    `type`: String,
    default: Boolean = false,
    user: Option[String] = Some("user"),
    password: Option[String] = None,
    host: Option[String] = None,
    port: Option[Int] = None,
    catalog: Option[String] = None,
    schema: Option[String] = None,
    useHttps: Option[Boolean] = None,
    properties: Map[String, Any] = Map.empty
):
  def dbType: DBType                                         = DBType.fromString(`type`)
  def withProperty(key: String, value: Any): ConnectorConfig = copy(properties =
    properties + (key -> value)
  )

/**
  * A named environment activating a set of connectors simultaneously. Selected with `--profile`;
  * one connector (explicit `default: true`, or the first one) is the default engine that queries
  * compile against.
  */
case class Profile(name: String, connectors: Seq[ConnectorConfig]):
  /**
    * The engine queries compile and execute against: the connector marked `default: true`, or the
    * first connector when none is marked.
    */
  def defaultEngine: ConnectorConfig = connectors
    .find(_.default)
    .orElse(connectors.headOption)
    .getOrElse(
      throw StatusCode.INVALID_ARGUMENT.newException(s"Profile '${name}' has no connectors")
    )

  /**
    * Apply CLI-level `--catalog`/`--schema` overrides to the default engine, leaving other
    * connectors untouched.
    */
  def withEngineOverrides(catalog: Option[String], schema: Option[String]): Profile =
    if catalog.isEmpty && schema.isEmpty then
      this
    else
      val engine  = defaultEngine
      val updated = engine.copy(
        catalog = catalog.orElse(engine.catalog),
        schema = schema.orElse(engine.schema)
      )
      copy(connectors =
        connectors.map { c =>
          if c eq engine then
            updated
          else
            c
        }
      )

  /** Add a property to the default engine's connector config */
  def withProperty(key: String, value: Any): Profile =
    val engine  = defaultEngine
    val updated = engine.withProperty(key, value)
    copy(connectors =
      connectors.map { c =>
        if c eq engine then
          updated
        else
          c
      }
    )

end Profile

object Profile extends LogSupport:

  // Matches both $VAR and ${VAR} forms anywhere in the file text.
  // Groups: 1 = bare $VAR; 2 = ${VAR}.
  private val envVarPattern = """\$\{([A-Za-z0-9_]+)\}|\$([A-Za-z0-9_]+)""".r

  // Wraps the top-level shape of profiles.json: {"profiles": [Profile, ...]}.
  // Private to keep the on-disk schema an implementation detail.
  private case class ProfileConfig(profiles: Seq[Profile] = Seq.empty)

  // Weaver's derivation falls back to Surface-driven weavers for field types with no given in
  // scope, and that runtime path decodes `Any` with a lossy fallback — every value in a
  // connector's `properties` map would silently become null. Provide the full given chain down
  // to Map[String, Any] (with AnyWeaver, which preserves primitive JSON values) so the macro
  // summons these instead.
  private given mapWeaver: Weaver[Map[String, Any]] = MapWeaver(
    summon[Weaver[String]],
    AnyWeaver.default
  ).asInstanceOf[Weaver[Map[String, Any]]]

  private given connectorConfigWeaver: Weaver[ConnectorConfig]         = Weaver.of[ConnectorConfig]
  private given connectorConfigSeqWeaver: Weaver[Seq[ConnectorConfig]] = SeqWeaver(
    connectorConfigWeaver,
    classOf[Seq[?]]
  ).asInstanceOf[Weaver[Seq[ConnectorConfig]]]

  private given profileWeaver: Weaver[Profile]         = Weaver.of[Profile]
  private given profileSeqWeaver: Weaver[Seq[Profile]] = SeqWeaver(profileWeaver, classOf[Seq[?]])
    .asInstanceOf[Weaver[Seq[Profile]]]

  def defaultGenericProfile = Profile(
    name = "local",
    connectors = Seq(ConnectorConfig(name = "generic", `type` = "generic"))
  )

  def defaultDuckDBProfile = Profile(
    name = "local",
    connectors = Seq(
      ConnectorConfig(
        name = "duckdb",
        `type` = "duckdb",
        catalog = Some("memory"),
        schema = Some("main")
      )
    )
  )

  def defaultProfileFor(dbType: DBType): Profile =
    dbType match
      case DBType.DuckDB =>
        defaultDuckDBProfile
      case other =>
        val tpe = other.toString.toLowerCase
        Profile(name = "local", connectors = Seq(ConnectorConfig(name = tpe, `type` = tpe)))

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

    currentProfile.withEngineOverrides(catalog, schema)

  end getProfile

  def getProfile(profile: String): Option[Profile] = getProfile(profile, defaultConfigDir)

  /**
    * Test-friendly overload that takes an explicit config directory instead of resolving
    * `${HOME}/.wvlet`. Production code calls the single-arg version.
    */
  def getProfile(profile: String, configDir: String): Option[Profile] =
    resolveConfigPath(configDir) match
      case Some(configPath) =>
        readProfileConfig(configPath).profiles.find(_.name == profile)
      case None =>
        None

  // Resolves `~/.wvlet` via uni's cross-platform `IO.homeDirectory`. JVM reads
  // `System.getProperty("user.home")`, Node uses `os.homedir()`, Native uses libc's
  // `getpwuid`/`HOME` fallback — so the profile config lives in the same place across all three.
  private def defaultConfigDir: String = s"${IO.homeDirectory}/.wvlet"

  // Returns the path of the first profile config file found, or None when
  // no config exists at all.
  //
  // If only a legacy YAML file is present we throw INVALID_ARGUMENT instead
  // of returning None — otherwise upstream `getProfile(Some(name), ...)` would
  // silently fall back to the local default and execute `--profile td-prod`
  // against DuckDB. Force the user to convert before any query runs.
  private def resolveConfigPath(configDir: String): Option[String] =
    val candidates = Seq("profiles.json", "profiles.jsonc")
    val found      = candidates.iterator.map(name => s"${configDir}/${name}").find(IO.isFile)
    found.orElse {
      Seq("profiles.yml", "profiles.yaml")
        .map(name => s"${configDir}/${name}")
        .find(IO.isFile)
        .foreach { path =>
          throw StatusCode
            .INVALID_ARGUMENT
            .newException(
              s"YAML profile config at ${path} is no longer supported. Convert it to ${configDir}/profiles.json " +
                s"(JSONC: comments and trailing commas allowed). Quick conversion: `yq -o=json ${path} > ${configDir}/profiles.json`."
            )
        }
      None
    }

  private def readProfileConfig(configPath: String): ProfileConfig =
    val rawText = IO.readString(configPath)
    val parsed  = JSON.parse(rawText)
    // Detect the pre-2026.2.0 flat shape BEFORE env-var expansion so the converted
    // JSON in the error message preserves ${VAR} placeholders instead of leaking secrets.
    failOnLegacyProfiles(parsed, configPath)
    val expanded = expandEnvVarsInStrings(parsed, configPath)
    Weaver.of[ProfileConfig].fromJSONValue(expanded)

  // Pre-2026.2.0 profiles were flat connection records ({"name": ..., "type": "trino", ...}).
  // 2026.2.0 made profiles multi-connector environments; instead of silently normalizing the
  // old shape forever, fail with the exact converted JSON so users can copy-paste once.
  private def failOnLegacyProfiles(parsed: JSONValue, configPath: String): Unit =
    val legacyProfiles =
      parsed match
        case o: JSONObject =>
          o.get("profiles") match
            case Some(JSONArray(profiles)) =>
              profiles.collect {
                case p: JSONObject if p.get("type").nonEmpty && p.get("connectors").isEmpty =>
                  p
              }
            case _ =>
              Nil
        case _ =>
          Nil
    if legacyProfiles.nonEmpty then
      val converted = legacyProfiles.map(p => convertLegacyProfile(p).toJSON).mkString(",\n")
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"""${configPath} uses the pre-2026.2.0 flat profile format, which is no longer supported.
             |Each profile now holds a `connectors` array of named connections. Replace the flat profile(s) with:
             |${converted}""".stripMargin)

  // Wrap all fields except `name` into a single connector named after the connection type.
  private def convertLegacyProfile(profile: JSONObject): JSONObject =
    val profileName = profile
      .get("name")
      .collect { case s: JSONString =>
        s
      }
      .getOrElse(JSONString("default"))
    val connectorName = profile
      .get("type")
      .collect { case JSONString(s) =>
        s
      }
      .getOrElse("generic")
    val connector = JSONObject(
      ("name" -> JSONString(connectorName)) +: profile.v.filterNot(_._1 == "name")
    )
    JSONObject(Seq("name" -> profileName, "connectors" -> JSONArray(IndexedSeq(connector))))

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

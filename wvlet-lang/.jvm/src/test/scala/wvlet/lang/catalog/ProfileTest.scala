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

import wvlet.uni.test.UniTest
import wvlet.uni.test.defined
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException

import java.nio.file.Files
import java.nio.file.Paths

class ProfileTest extends UniTest:

  private def withMockHome[A](profileFileName: String, profileContent: String)(
      testCode: String => A
  ): A =
    val targetDir = Paths.get("target/test-temp")
    Files.createDirectories(targetDir)
    val mockHomeDir = Files.createTempDirectory(targetDir, "mock_home")
    val wvletDir    = mockHomeDir.resolve(".wvlet")
    Files.createDirectories(wvletDir)
    Files.writeString(wvletDir.resolve(profileFileName), profileContent)

    val originalHome = sys.props.get("user.home")
    try
      sys.props("user.home") = mockHomeDir.toString
      testCode(mockHomeDir.toString)
    finally
      originalHome match
        case Some(home) =>
          sys.props("user.home") = home
        case None =>
          sys.props.remove("user.home")
      Files.walk(mockHomeDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)

  test("should handle missing environment variables properly") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "test",
        |      "connectors": [
        |        { "name": "duckdb", "type": "duckdb", "host": "$MISSING_ENV_VAR", "port": 5432 }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val exception = intercept[WvletLangException] {
        Profile.getProfile("test")
      }

      exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
      exception.message shouldContain "Environment variable 'MISSING_ENV_VAR' is not set"
      exception.message shouldContain "profile configuration"
    }
  }

  test("should resolve environment variables correctly when they exist") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "test",
        |      "connectors": [
        |        { "name": "duckdb", "type": "duckdb", "host": "$USER", "port": 5432 }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("test")
      profile shouldBe defined
      profile.get.defaultEngine.host shouldBe sys.env.get("USER")
    }
  }

  test("should handle profiles without environment variables") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "simple",
        |      "connectors": [
        |        { "name": "duckdb", "type": "duckdb", "host": "localhost", "port": 5432 }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("simple")
      profile shouldBe defined
      profile.get.defaultEngine.host shouldBe Some("localhost")
      profile.get.defaultEngine.port shouldBe Some(5432)
    }
  }

  test("should handle missing environment variables with braces format") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "test",
        |      "connectors": [
        |        { "name": "duckdb", "type": "duckdb", "host": "${MISSING_BRACES_VAR}", "port": 5432 }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val exception = intercept[WvletLangException] {
        Profile.getProfile("test")
      }

      exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
      exception.message shouldContain "Environment variable 'MISSING_BRACES_VAR' is not set"
      exception.message shouldContain "profile configuration"
    }
  }

  test("should resolve environment variables with braces format") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "test",
        |      "connectors": [
        |        { "name": "duckdb", "type": "duckdb", "host": "${USER}", "port": 5432 }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("test")
      profile shouldBe defined
      profile.get.defaultEngine.host shouldBe sys.env.get("USER")
    }
  }

  test("should handle mixed valid and invalid patterns correctly") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "test",
        |      "connectors": [
        |        { "name": "duckdb", "type": "duckdb", "host": "$USER}_suffix", "port": 5432 }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("test")
      profile shouldBe defined
      // $USER should be substituted; the trailing }_suffix stays literal.
      val expectedHost = sys.env.get("USER").map(_ + "}_suffix")
      profile.get.defaultEngine.host shouldBe expectedHost
    }
  }

  test("should accept JSONC comments and trailing commas") {
    val profileContent =
      """// top-level comment
        |{
        |  /* the profiles array */
        |  "profiles": [
        |    {
        |      "name": "with_comments",
        |      "connectors": [
        |        {
        |          "name": "duckdb",
        |          "type": "duckdb",
        |          "host": "localhost", // trailing comment
        |          "port": 5432, /* trailing comma allowed below */
        |        },
        |      ],
        |    },
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("with_comments")
      profile shouldBe defined
      profile.get.defaultEngine.host shouldBe Some("localhost")
      profile.get.defaultEngine.port shouldBe Some(5432)
    }
  }

  test("should also accept .jsonc extension") {
    val profileContent =
      """{ "profiles": [ { "name": "alt_ext", "connectors": [ { "name": "duckdb", "type": "duckdb", "host": "h" } ] } ] }"""

    withMockHome("profiles.jsonc", profileContent) { _ =>
      val profile = Profile.getProfile("alt_ext")
      profile shouldBe defined
      profile.get.defaultEngine.host shouldBe Some("h")
    }
  }

  test("should throw INVALID_ARGUMENT when only profiles.yml exists") {
    withMockHome("profiles.yml", "profiles:\n  - name: legacy\n    type: duckdb\n") { _ =>
      val exception = intercept[WvletLangException] {
        Profile.getProfile("legacy")
      }
      exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
      exception.message shouldContain "no longer supported"
      exception.message shouldContain "profiles.json"
      exception.message shouldContain "yq -o=json"
    }
  }

  test("should throw INVALID_ARGUMENT when only profiles.yaml (long extension) exists") {
    withMockHome("profiles.yaml", "profiles:\n  - name: legacy2\n    type: duckdb\n") { _ =>
      val exception = intercept[WvletLangException] {
        Profile.getProfile("legacy2")
      }
      exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    }
  }

  test("should ignore env-var syntax inside JSONC comments") {
    val profileContent =
      """{
        |  // example: ${WVLET_NEVER_DEFINED_VAR_42}
        |  /* also: $WVLET_NEVER_DEFINED_VAR_43 */
        |  "profiles": [
        |    {
        |      "name": "no_envs_in_comments",
        |      "connectors": [ { "name": "duckdb", "type": "duckdb", "host": "localhost" } ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("no_envs_in_comments")
      profile shouldBe defined
      profile.get.defaultEngine.host shouldBe Some("localhost")
    }
  }

  test("should ignore env-var syntax in non-string JSON keys/positions") {
    // A user might use a `$schema` key for editor tooling — must not be interpreted as env lookup.
    val profileContent =
      """{
        |  "$schema": "https://example.com/profiles.schema.json",
        |  "profiles": [
        |    {
        |      "name": "with_schema_key",
        |      "connectors": [ { "name": "duckdb", "type": "duckdb", "host": "localhost" } ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("with_schema_key")
      profile shouldBe defined
      profile.get.defaultEngine.host shouldBe Some("localhost")
    }
  }

  test("should reject the pre-2026.2.0 flat profile format with a converted example") {
    val profileContent =
      """{
        |  "profiles": [
        |    { "name": "td", "type": "trino", "host": "api.example.com", "password": "${TD_API_KEY}" }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val exception = intercept[WvletLangException] {
        Profile.getProfile("td")
      }
      exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
      exception.message shouldContain "pre-2026.2.0 flat profile format"
      // The error prints the converted JSON: the profile wrapping a single connector named
      // after its type
      exception.message shouldContain "\"connectors\""
      exception.message shouldContain "\"name\":\"trino\""
      // Detection runs before env-var expansion: placeholders are preserved and secrets
      // never reach the error message
      exception.message shouldContain "${TD_API_KEY}"
    }
  }

  test("should select the connector marked default: true as the default engine") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "multi",
        |      "connectors": [
        |        { "name": "local", "type": "duckdb", "catalog": "memory" },
        |        { "name": "td", "type": "trino", "default": true, "host": "api.example.com" }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("multi")
      profile shouldBe defined
      profile.get.connectors.size shouldBe 2
      profile.get.defaultEngine.name shouldBe "td"
      profile.get.defaultEngine.`type` shouldBe "trino"
    }
  }

  test("should default to the first connector when no default flag is set") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "multi",
        |      "connectors": [
        |        { "name": "local", "type": "duckdb", "catalog": "memory" },
        |        { "name": "td", "type": "trino", "host": "api.example.com" }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("multi")
      profile shouldBe defined
      profile.get.defaultEngine.name shouldBe "local"
      // `default` omitted in JSON decodes to false
      profile.get.connectors.forall(_.default == false) shouldBe true
    }
  }

  test("should apply catalog/schema overrides to the default engine only") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "multi",
        |      "connectors": [
        |        { "name": "td", "type": "trino", "default": true, "catalog": "td", "schema": "public" },
        |        { "name": "local", "type": "duckdb", "catalog": "memory", "schema": "main" }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile(
        Some("multi"),
        catalog = Some("overridden"),
        schema = Some("s2")
      )
      profile.defaultEngine.catalog shouldBe Some("overridden")
      profile.defaultEngine.schema shouldBe Some("s2")
      // Non-default connectors are untouched
      val local = profile.connectors.find(_.name == "local").get
      local.catalog shouldBe Some("memory")
      local.schema shouldBe Some("main")
    }
  }

  test("should read connector properties on non-default connectors") {
    val profileContent =
      """{
        |  "profiles": [
        |    {
        |      "name": "multi",
        |      "connectors": [
        |        { "name": "duckdb", "type": "duckdb" },
        |        { "name": "td", "type": "trino", "properties": { "useSSL": false, "retries": 3 } }
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("multi")
      profile shouldBe defined
      val td = profile.get.connectors.find(_.name == "td").get
      td.properties.get("useSSL") shouldBe Some(false)
      td.properties.get("retries") shouldBe Some(3)
    }
  }

  test("should fail with INVALID_ARGUMENT when a profile has no connectors") {
    val profile   = Profile(name = "empty", connectors = Seq.empty)
    val exception = intercept[WvletLangException] {
      profile.defaultEngine
    }
    exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    exception.message shouldContain "no connectors"
  }

end ProfileTest

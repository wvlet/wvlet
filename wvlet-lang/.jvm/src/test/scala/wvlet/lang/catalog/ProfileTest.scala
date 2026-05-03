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
import wvlet.uni.test.{defined, empty}
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
        |    { "name": "test", "type": "duckdb", "host": "$MISSING_ENV_VAR", "port": 5432 }
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
        |    { "name": "test", "type": "duckdb", "host": "$USER", "port": 5432 }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("test")
      profile shouldBe defined
      profile.get.host shouldBe sys.env.get("USER")
    }
  }

  test("should handle profiles without environment variables") {
    val profileContent =
      """{
        |  "profiles": [
        |    { "name": "simple", "type": "duckdb", "host": "localhost", "port": 5432 }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("simple")
      profile shouldBe defined
      profile.get.host shouldBe Some("localhost")
      profile.get.port shouldBe Some(5432)
    }
  }

  test("should handle missing environment variables with braces format") {
    val profileContent =
      """{
        |  "profiles": [
        |    { "name": "test", "type": "duckdb", "host": "${MISSING_BRACES_VAR}", "port": 5432 }
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
        |    { "name": "test", "type": "duckdb", "host": "${USER}", "port": 5432 }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("test")
      profile shouldBe defined
      profile.get.host shouldBe sys.env.get("USER")
    }
  }

  test("should handle mixed valid and invalid patterns correctly") {
    val profileContent =
      """{
        |  "profiles": [
        |    { "name": "test", "type": "duckdb", "host": "$USER}_suffix", "port": 5432 }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("test")
      profile shouldBe defined
      // $USER should be substituted; the trailing }_suffix stays literal.
      val expectedHost = sys.env.get("USER").map(_ + "}_suffix")
      profile.get.host shouldBe expectedHost
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
        |      "type": "duckdb",
        |      "host": "localhost", // trailing comment
        |      "port": 5432, /* trailing comma allowed below */
        |    },
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("with_comments")
      profile shouldBe defined
      profile.get.host shouldBe Some("localhost")
      profile.get.port shouldBe Some(5432)
    }
  }

  test("should also accept .jsonc extension") {
    val profileContent =
      """{ "profiles": [ { "name": "alt_ext", "type": "duckdb", "host": "h" } ] }"""

    withMockHome("profiles.jsonc", profileContent) { _ =>
      val profile = Profile.getProfile("alt_ext")
      profile shouldBe defined
      profile.get.host shouldBe Some("h")
    }
  }

  test("should warn and return None when only profiles.yml exists") {
    withMockHome("profiles.yml", "profiles:\n  - name: legacy\n    type: duckdb\n") { _ =>
      val profile = Profile.getProfile("legacy")
      profile shouldBe empty
    }
  }

  test("should ignore env-var syntax inside JSONC comments") {
    val profileContent =
      """{
        |  // example: ${WVLET_NEVER_DEFINED_VAR_42}
        |  /* also: $WVLET_NEVER_DEFINED_VAR_43 */
        |  "profiles": [
        |    { "name": "no_envs_in_comments", "type": "duckdb", "host": "localhost" }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("no_envs_in_comments")
      profile shouldBe defined
      profile.get.host shouldBe Some("localhost")
    }
  }

  test("should ignore env-var syntax in non-string JSON keys/positions") {
    // A user might use a `$schema` key for editor tooling — must not be interpreted as env lookup.
    val profileContent =
      """{
        |  "$schema": "https://example.com/profiles.schema.json",
        |  "profiles": [
        |    { "name": "with_schema_key", "type": "duckdb", "host": "localhost" }
        |  ]
        |}""".stripMargin

    withMockHome("profiles.json", profileContent) { _ =>
      val profile = Profile.getProfile("with_schema_key")
      profile shouldBe defined
      profile.get.host shouldBe Some("localhost")
    }
  }

end ProfileTest

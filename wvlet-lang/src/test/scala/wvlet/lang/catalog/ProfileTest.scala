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

import wvlet.airspec.AirSpec
import wvlet.lang.api.{StatusCode, WvletLangException}

import java.io.File
import java.nio.file.{Files, Path, Paths}

class ProfileTest extends AirSpec:

  private def withTempProfileFile[A](content: String)(f: String => A): A =
    val targetDir = Paths.get("target/test-temp")
    Files.createDirectories(targetDir)
    val tempFile = Files.createTempFile(targetDir, "profiles", ".yml")
    try
      Files.writeString(tempFile, content)
      f(tempFile.toString)
    finally
      Files.deleteIfExists(tempFile)

  test("should handle missing environment variables properly") {
    val profileContent =
      """
        |profiles:
        |  - name: test
        |    type: duckdb
        |    host: $MISSING_ENV_VAR
        |    port: 5432
        |""".stripMargin

    withTempProfileFile(profileContent) { tempPath =>
      // Mock the profiles.yml path by temporarily setting the home directory
      val originalHome = sys.props.get("user.home")
      val tempDir      = Paths.get(tempPath).getParent
      val mockHomeDir  = tempDir.resolve("mock_home")
      Files.createDirectories(mockHomeDir.resolve(".wvlet"))
      Files.copy(Paths.get(tempPath), mockHomeDir.resolve(".wvlet").resolve("profiles.yml"))

      try
        sys.props("user.home") = mockHomeDir.toString

        val exception = intercept[WvletLangException] {
          Profile.getProfile("test")
        }

        exception.statusCode shouldBe StatusCode.INVALID_ARGUMENT
        exception.message shouldContain "Environment variable 'MISSING_ENV_VAR' is not set"
        exception.message shouldContain "profile configuration"

      finally
        // Restore original home directory
        originalHome match
          case Some(home) =>
            sys.props("user.home") = home
          case None =>
            sys.props.remove("user.home")
        // Clean up mock home directory
        Files.walk(mockHomeDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)
    }
  }

  test("should resolve environment variables correctly when they exist") {
    // Use an environment variable that should exist on most systems
    val profileContent =
      """
        |profiles:
        |  - name: test
        |    type: duckdb
        |    host: $USER
        |    port: 5432
        |""".stripMargin

    withTempProfileFile(profileContent) { tempPath =>
      val tempDir     = Paths.get(tempPath).getParent
      val mockHomeDir = tempDir.resolve("mock_home")
      Files.createDirectories(mockHomeDir.resolve(".wvlet"))
      Files.copy(Paths.get(tempPath), mockHomeDir.resolve(".wvlet").resolve("profiles.yml"))

      val originalHome = sys.props.get("user.home")
      try
        sys.props("user.home") = mockHomeDir.toString

        val profile = Profile.getProfile("test")
        profile shouldBe defined
        // The host should be set to the value of the USER environment variable
        profile.get.host shouldBe defined
        profile.get.host shouldNotBe Some("$USER") // Should be resolved

      finally
        // Restore original home directory
        originalHome match
          case Some(home) =>
            sys.props("user.home") = home
          case None =>
            sys.props.remove("user.home")
        Files.walk(mockHomeDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)
    }
  }

  test("should handle profiles without environment variables") {
    val profileContent =
      """
        |profiles:
        |  - name: simple
        |    type: duckdb
        |    host: localhost
        |    port: 5432
        |""".stripMargin

    withTempProfileFile(profileContent) { tempPath =>
      val tempDir     = Paths.get(tempPath).getParent
      val mockHomeDir = tempDir.resolve("mock_home")
      Files.createDirectories(mockHomeDir.resolve(".wvlet"))
      Files.copy(Paths.get(tempPath), mockHomeDir.resolve(".wvlet").resolve("profiles.yml"))

      try
        sys.props("user.home") = mockHomeDir.toString

        val profile = Profile.getProfile("simple")
        profile shouldBe defined
        profile.get.host shouldBe Some("localhost")
        profile.get.port shouldBe Some(5432)

      finally
        Files.walk(mockHomeDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)
    }
  }

end ProfileTest

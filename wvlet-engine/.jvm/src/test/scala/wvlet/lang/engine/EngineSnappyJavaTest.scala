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
package wvlet.lang.engine

import wvlet.uni.test.UniTest

/**
  * The JVM engine depends on snappy-java, so uni decodes Snappy pages with its native library
  * unless `-Dwvlet.uni.snappy.useSnappyJava=false` turns it off. A platform without a fitting
  * native library falls back to uni's own decoder, which this test would catch as a failure.
  */
class EngineSnappyJavaTest extends UniTest:

  test("uni decodes Snappy with snappy-java on the JVM") {
    // SnappyJava is package-private in uni, so read its flag by reflection
    val field = Class.forName("wvlet.uni.io.codec.SnappyJava").getDeclaredField("available")
    field.setAccessible(true)
    val turnedOff = sys
      .props
      .get("wvlet.uni.snappy.useSnappyJava")
      .exists(_.equalsIgnoreCase("false"))
    field.getBoolean(null) shouldBe !turnedOff
  }

end EngineSnappyJavaTest

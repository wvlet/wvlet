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
package wvlet.lang.compiler

import wvlet.uni.test.UniTest

class DataFilePathResolutionTest extends UniTest:
  private val context = Context.testGlobalContext("work").getContextOf(CompilationUnit.empty)

  test("resolve a relative data file path against the work folder") {
    context.dataFilePath("data/t.csv") shouldBe "work/data/t.csv"
  }

  test("keep an absolute data file path as is") {
    context.dataFilePath("/abs/path/t.csv") shouldBe "/abs/path/t.csv"
    context.dataFilePath("C:\\data\\t.csv") shouldBe "C:\\data\\t.csv"
    context.dataFilePath("C:/data/t.csv") shouldBe "C:/data/t.csv"
  }

  test("keep a remote data file URL as is") {
    context.dataFilePath("s3://bucket/t.parquet") shouldBe "s3://bucket/t.parquet"
    context.dataFilePath("https://example.com/t.csv") shouldBe "https://example.com/t.csv"
    context.dataFilePath("gs://bucket/t.parquet") shouldBe "gs://bucket/t.parquet"
  }

end DataFilePathResolutionTest

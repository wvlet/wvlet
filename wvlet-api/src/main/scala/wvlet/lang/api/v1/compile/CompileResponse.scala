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
package wvlet.lang.api.v1.compile

import wvlet.lang.api.StatusCode

/**
  * Response format for compilation APIs
  */
case class CompileResponse(
    success: Boolean,
    sql: Option[String] = None,
    error: Option[CompileError] = None
)

/**
  * Error information for compilation failures
  */
case class CompileError(
    statusCode: StatusCode,
    message: String,
    location: Option[ErrorLocation] = None
)

/**
  * Source location information for compilation errors
  */
case class ErrorLocation(
    path: String,     // Relative file path (e.g., "src/main/scala/Example.scala")
    fileName: String, // Leaf file name only (e.g., "Example.scala")
    line: Int,        // 1-origin line number
    column: Int,      // 1-origin column number
    lineContent: Option[String] = None
)

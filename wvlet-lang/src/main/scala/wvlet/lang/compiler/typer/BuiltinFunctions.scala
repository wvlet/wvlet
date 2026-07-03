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
package wvlet.lang.compiler.typer

import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.*
import wvlet.lang.model.expr.FunctionArg

/**
  * Return-type signatures of well-known SQL functions that have no definition in the standard
  * library (following the Definitions concept of the Scala 3 compiler). Used by the Typer to assign
  * a best-effort type to function applications that are passed through to the target database
  * engine. Only functions whose return type is consistent across the supported engines (DuckDB,
  * Trino) are listed
  */
object BuiltinFunctions:

  private val longFunctions = Set(
    "count",
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "length",
    "strlen",
    "hash"
  )

  private val booleanFunctions = Set("regexp_matches", "regexp_like", "contains", "starts_with")

  private val stringFunctions = Set(
    "concat",
    "upper",
    "lower",
    "trim",
    "ltrim",
    "rtrim",
    "substring",
    "substr",
    "replace",
    "regexp_replace",
    "reverse",
    "lpad",
    "rpad",
    "string_agg",
    "strftime"
  )

  private val doubleFunctions = Set("avg", "stddev", "stddev_samp", "stddev_pop", "sqrt")

  /**
    * Functions whose return type follows the type of their first argument
    */
  private val firstArgTypeFunctions = Set(
    "min",
    "max",
    "sum",
    "abs",
    "coalesce",
    "greatest",
    "least",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "arbitrary",
    "any_value",
    "round"
  )

  /**
    * Returns the return type of a well-known SQL function, or None when the function is not
    * recognized or its return type cannot be derived from the arguments
    */
  def returnTypeOf(name: String, args: List[FunctionArg]): Option[DataType] =
    val fn = name.toLowerCase
    if fn == "unnest" then
      // unnest(array(T)) yields values of the element type T
      args
        .headOption
        .map(_.value.dataType)
        .collect {
          case ArrayType(elem) if elem.isResolved =>
            elem
        }
    else if longFunctions.contains(fn) then
      Some(LongType)
    else if booleanFunctions.contains(fn) then
      Some(BooleanType)
    else if stringFunctions.contains(fn) then
      Some(StringType)
    else if doubleFunctions.contains(fn) then
      Some(DoubleType)
    else if firstArgTypeFunctions.contains(fn) then
      args.headOption.map(_.value.dataType).filter(_.isResolved)
    else
      None

end BuiltinFunctions

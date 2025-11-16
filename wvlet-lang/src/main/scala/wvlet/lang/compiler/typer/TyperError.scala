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

import wvlet.lang.api.Span
import wvlet.lang.api.SourceLocation
import wvlet.lang.compiler.Context
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.expr.Identifier
import wvlet.lang.model.Type

/**
  * Base trait for typing errors
  */
sealed trait TyperError:
  def message: String
  def span: Span
  def sourceLocation(using ctx: Context): SourceLocation = ctx.sourceLocationAt(span)

/**
  * Error for unresolved identifiers
  */
case class UnresolvedIdentifier(id: Identifier) extends TyperError:
  def message: String = s"Unresolved identifier: ${id.leafName}"
  def span: Span      = id.span

/**
  * Error for type mismatches
  */
case class TypeMismatch(expected: Type, actual: Type, expr: Expression) extends TyperError:
  def message: String = s"Type mismatch: expected $expected, got $actual"
  def span: Span      = expr.span

/**
  * Error for unresolved table references
  */
case class UnresolvedTable(name: String, span: Span) extends TyperError:
  def message: String = s"Unresolved table: $name"

/**
  * Error for invalid filter conditions
  */
case class InvalidFilterCondition(actualType: Type, expr: Expression) extends TyperError:
  def message: String = s"Filter condition must be boolean, got: $actualType"
  def span: Span      = expr.span

/**
  * Generic typing error
  */
case class GenericTyperError(message: String, span: Span) extends TyperError

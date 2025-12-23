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

import wvlet.lang.compiler.TypeName
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.ErrorType
import wvlet.lang.model.Type.NoType
import wvlet.lang.model.Type.TypeVar

/**
  * Type inference utilities for the typer phase.
  *
  * Handles:
  *   - Common type finding (for CASE/WHEN, UNION, etc.)
  *   - Type coercion rules (implicit type conversions)
  *   - Type unification (for generic type instantiation)
  *   - NULL type handling
  */
object TypeInference:

  /**
    * Numeric type promotion order (widest first)
    */
  private val numericPromotionOrder: Seq[DataType] =
    Seq(DoubleType, FloatType, LongType, IntType)

  /**
    * Find the common type among a list of types. Used for CASE/WHEN branches, UNION columns, etc.
    *
    * Rules:
    *   1. If all types are the same, return that type
    *   2. NULL can be coerced to any type
    *   3. Numeric types are promoted to the widest type
    *   4. String concatenation allows mixed types (coerced to string)
    *   5. ErrorType propagates
    */
  def findCommonType(types: Seq[Type]): Type =
    if types.isEmpty then
      NoType
    else
      // Filter out NullType - it can be coerced to any type
      val nonNullTypes = types.filter(_ != NullType)
      if nonNullTypes.isEmpty then
        // All types are NULL
        NullType
      else if nonNullTypes.forall(_ == nonNullTypes.head) then
        // All non-null types are the same
        nonNullTypes.head
      else
        // Check for error types first
        types.collectFirst { case e: ErrorType => e } match
          case Some(error) =>
            error
          case None =>
            // Try numeric promotion
            findNumericCommonType(nonNullTypes)
              .orElse(findStringCommonType(nonNullTypes))
              .getOrElse(ErrorType(s"No common type found among: ${types.mkString(", ")}"))

  /**
    * Find common numeric type by promotion
    */
  private def findNumericCommonType(types: Seq[Type]): Option[Type] =
    val allNumeric = types.forall {
      case IntType | LongType | FloatType | DoubleType => true
      case _                                           => false
    }
    if allNumeric then
      numericPromotionOrder.find(t => types.exists(_ == t))
    else
      None

  /**
    * Find common string type - only if all types are string
    *
    * Note: String concatenation coercion (where string absorbs other types) should be handled
    * separately in binary operation typing, not in general common type finding.
    */
  private def findStringCommonType(types: Seq[Type]): Option[Type] =
    if types.forall(_ == StringType) then
      Some(StringType)
    else
      None

  /**
    * Check if a type can be implicitly coerced to another type.
    *
    * @param from
    *   source type
    * @param to
    *   target type
    * @return
    *   true if implicit coercion is allowed
    */
  def canCoerce(from: Type, to: Type): Boolean =
    if from == to then
      true
    else
      (from, to) match
        // NULL can be coerced to any type
        case (NullType, _) =>
          true
        // NoType means untyped - allow coercion during inference
        case (NoType, _) | (_, NoType) =>
          true
        // Numeric promotions (widening)
        case (IntType, LongType | FloatType | DoubleType)   => true
        case (LongType, FloatType | DoubleType)             => true
        case (FloatType, DoubleType)                        => true
        // String coercion (for concatenation)
        case (IntType | LongType | FloatType | DoubleType | BooleanType, StringType) =>
          true
        // TypeVar can be coerced to any type (will be resolved later)
        case (_: TypeVar, _) | (_, _: TypeVar) =>
          true
        case _ =>
          false

  /**
    * Widen a type to a target type if coercion is allowed.
    *
    * @param from
    *   source type
    * @param to
    *   target type
    * @return
    *   the widened type, or ErrorType if coercion is not allowed
    */
  def widen(from: Type, to: Type): Type =
    if canCoerce(from, to) then
      to
    else
      ErrorType(s"Cannot coerce ${from} to ${to}")

  /**
    * Unify two types, returning their common supertype or an error.
    *
    * This is used for:
    *   - Binary operations (finding result type)
    *   - Function argument matching
    *   - Generic type instantiation
    *
    * @param t1
    *   first type
    * @param t2
    *   second type
    * @return
    *   unified type or ErrorType
    */
  def unify(t1: Type, t2: Type): Type =
    if t1 == t2 then
      t1
    else
      (t1, t2) match
        // NoType unifies with anything
        case (NoType, t) => t
        case (t, NoType) => t
        // NULL unifies to the other type
        case (NullType, t) => t
        case (t, NullType) => t
        // TypeVar unifies with any type (placeholder for inference)
        case (_: TypeVar, t) => t
        case (t, _: TypeVar) => t
        // Numeric unification - promote to wider type
        case (IntType, LongType) | (LongType, IntType)     => LongType
        case (IntType, FloatType) | (FloatType, IntType)   => FloatType
        case (IntType, DoubleType) | (DoubleType, IntType) => DoubleType
        case (LongType, FloatType) | (FloatType, LongType) => FloatType
        case (LongType, DoubleType) | (DoubleType, LongType) => DoubleType
        case (FloatType, DoubleType) | (DoubleType, FloatType) => DoubleType
        // ErrorType propagates
        case (e: ErrorType, _) => e
        case (_, e: ErrorType) => e
        // No unification possible
        case _ =>
          ErrorType(s"Cannot unify types: ${t1} and ${t2}")

  /**
    * Bind type variables in a type using a substitution map.
    *
    * @param tpe
    *   the type to bind
    * @param substitution
    *   mapping from type variable IDs to concrete types
    * @return
    *   the type with variables substituted
    */
  def bindTypeVars(tpe: Type, substitution: Map[Int, Type]): Type =
    tpe match
      case TypeVar(id) =>
        substitution.getOrElse(id, tpe)
      case dt: DataType =>
        // DataType has its own bind method for TypeName-based binding
        dt
      case _ =>
        tpe

end TypeInference

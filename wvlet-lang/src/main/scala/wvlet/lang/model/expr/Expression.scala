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
package wvlet.lang.model.expr

import wvlet.lang.StatusCode
import wvlet.lang.model.*
import wvlet.lang.model.DataType.*
import wvlet.lang.model.plan.{LogicalPlan, LogicalPlanPrinter}
import wvlet.airframe.surface.reflect.ReflectTypeUtil
import wvlet.lang.model.NodeLocation.NoLocation
import wvlet.log.LogSupport

/**
  */
trait Expression extends TreeNode with Product with LogSupport:
  def pp: String = LogicalPlanPrinter.printExpression(this)
  def children: Seq[Expression]

  /**
    * Column name without qualifier
    * @return
    */
  def attributeName: String = "?"
  def dataTypeName: String  = dataType.typeDescription
  def dataType: DataType    = DataType.UnknownType

  protected def copyInstance(newArgs: Seq[AnyRef]): this.type =
    try
      val primaryConstructor = this.getClass.getDeclaredConstructors()(0)
      val newObj =
        if primaryConstructor.getParameterCount == 0 && this.getClass.getSimpleName.endsWith("$")
        then
          ReflectTypeUtil.companionObject(this.getClass).get
        else
          primaryConstructor.newInstance(newArgs*)
      newObj.asInstanceOf[this.type]
    catch
      case e: IllegalArgumentException =>
        throw StatusCode
          .NON_RETRYABLE_INTERNAL_ERROR
          .newException(
            s"Failed to create a new instance for ${this.getClass.getSimpleName} with args [${newArgs.mkString(", ")}]",
            e
          )

  def transformPlan(rule: PartialFunction[LogicalPlan, LogicalPlan]): Expression =
    def recursiveTransform(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          e.transformPlan(rule)
        case l: LogicalPlan =>
          l.transform(rule)
        case Some(x) =>
          Some(recursiveTransform(x))
        case s: Seq[?] =>
          s.map(recursiveTransform _)
        case other: AnyRef =>
          other
        case null =>
          null

    val newArgs = productIterator.map(recursiveTransform).toIndexedSeq
    copyInstance(newArgs)

  def traversePlan[U](rule: PartialFunction[LogicalPlan, U]): Unit =
    def recursiveTraverse(arg: Any): Unit =
      arg match
        case e: Expression =>
          e.traversePlan(rule)
        case l: LogicalPlan =>
          l.traverse(rule)
        case Some(x) =>
          Some(recursiveTraverse(x))
        case s: Seq[?] =>
          s.map(recursiveTraverse _)
        case other: AnyRef =>
        case null          =>
    productIterator.foreach(recursiveTraverse)

  def traversePlanOnce[U](rule: PartialFunction[LogicalPlan, U]): Unit =
    def recursiveTraverse(arg: Any): Unit =
      arg match
        case e: Expression =>
          e.traversePlanOnce(rule)
        case l: LogicalPlan =>
          l.traverseOnce(rule)
        case Some(x) =>
          Some(recursiveTraverse(x))
        case s: Seq[?] =>
          s.map(recursiveTraverse _)
        case other: AnyRef =>
        case null          =>
    productIterator.foreach(recursiveTraverse)

  /**
    * Recursively transform the expression in breadth-first order
    * @param rule
    * @return
    */
  def transformExpression(rule: PartialFunction[Expression, Expression]): Expression =
    var changed = false
    def recursiveTransform(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          val newPlan = e.transformExpression(rule)
          if e eq newPlan then
            e
          else
            changed = true
            newPlan
        case l: LogicalPlan =>
          val newPlan = l.transformExpressions(rule)
          if l eq newPlan then
            l
          else
            changed = true
            newPlan
        case Some(x) =>
          Some(recursiveTransform(x))
        case s: Seq[?] =>
          s.map(recursiveTransform)
        case other: AnyRef =>
          other
        case null =>
          null

    // First apply the rule to itself
    val newExpr: Expression = rule.applyOrElse(this, identity[Expression])

    // Next, apply the rule to child nodes
    val newArgs = newExpr.productIterator.map(recursiveTransform).toIndexedSeq
    if changed then
      newExpr.copyInstance(newArgs)
    else
      newExpr

  end transformExpression

  /**
    * Recursively transform the expression in depth-first order
    * @param rule
    * @return
    */
  def transformUpExpression(rule: PartialFunction[Expression, Expression]): Expression =
    var changed = false
    def iter(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          val newPlan = e.transformUpExpression(rule)
          if e eq newPlan then
            e
          else
            changed = true
            newPlan
        case l: LogicalPlan =>
          val newPlan = l.transformUpExpressions(rule)
          if l eq newPlan then
            l
          else
            changed = true
            newPlan
        case Some(x) =>
          Some(iter(x))
        case s: Seq[?] =>
          s.map(iter)
        case other: AnyRef =>
          other
        case null =>
          null

    // Apply the rule first to child nodes
    val newArgs = productIterator.map(iter).toIndexedSeq
    val newExpr =
      if changed then
        copyInstance(newArgs)
      else
        this

    // Finally, apply the rule to itself
    rule.applyOrElse(newExpr, identity[Expression])

  end transformUpExpression

  def transformChildExpressions(rule: PartialFunction[Expression, Expression]): Expression =
    var changed = false

    def iterOnce(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          val newExpr = e.transformUpExpression(rule)
          if e eq newExpr then
            e
          else
            changed = true
            newExpr
        case l: LogicalPlan =>
          val newPlan = l.transformChildExpressions(rule)
          if l eq newPlan then
            l
          else
            changed = true
            newPlan
        case Some(x) =>
          Some(iterOnce(x))
        case s: Seq[?] =>
          s.map(iterOnce)
        case other: AnyRef =>
          other
        case null =>
          null

    val newArgs = productIterator.map(iterOnce).toIndexedSeq
    if changed then
      copyInstance(newArgs)
    else
      this

  end transformChildExpressions

  def collectSubExpressions: List[Expression] =
    def recursiveCollect(arg: Any): List[Expression] =
      arg match
        case e: Expression =>
          e :: e.collectSubExpressions
        case l: LogicalPlan =>
          l.inputExpressions
        case Some(x) =>
          recursiveCollect(x)
        case s: Seq[?] =>
          s.flatMap(recursiveCollect _).toList
        case other: AnyRef =>
          Nil
        case null =>
          Nil

    productIterator.flatMap(recursiveCollect).toList

  def traverseExpressions[U](rule: PartialFunction[Expression, U]): Unit =
    def recursiveTraverse(arg: Any): Unit =
      arg match
        case e: Expression =>
          e.traverseExpressions(rule)
        case l: LogicalPlan =>
          l.traverseExpressions(rule)
        case Some(x) =>
          recursiveTraverse(x)
        case s: Seq[?] =>
          s.foreach(recursiveTraverse _)
        case other: AnyRef =>
        case null          =>

    if rule.isDefinedAt(this) then
      rule.apply(this)
    // Unlike transform, this will traverse the selected children by the Expression
    children.foreach(recursiveTraverse)

  def collectExpressions(cond: PartialFunction[Expression, Boolean]): List[Expression] =
    val l = List.newBuilder[Expression]
    traverseExpressions(
      new PartialFunction[Expression, Unit]:
        override def isDefinedAt(x: Expression): Boolean = cond.isDefinedAt(x)
        override def apply(v1: Expression): Unit =
          if cond.apply(v1) then
            l += v1
    )
    l.result()

  lazy val resolved: Boolean    = resolvedChildren
  def unresolved: Boolean       = !resolved
  def resolvedChildren: Boolean = children.forall(_.resolved) && resolvedInputs
  def resolvedInputs: Boolean   = dataType.isResolved

end Expression

trait LeafExpression extends Expression:
  override def children: Seq[Expression] = Nil

trait UnaryExpression extends Expression:
  def child: Expression
  override def children: Seq[Expression] = Seq(child)

trait BinaryExpression extends Expression:
  def left: Expression
  def right: Expression
  def operatorName: String

  override def dataType: DataType = DataType.BooleanType

  override def children: Seq[Expression] = Seq(left, right)
  override def toString: String = s"${getClass.getSimpleName}(left:${left}, right:${right})"

object Expression:
  def concat(expr: Seq[Expression])(merger: (Expression, Expression) => Expression): Expression =
    require(expr.length > 0, None)
    if expr.length == 1 then
      expr.head
    else
      expr
        .tail
        .foldLeft(expr.head) { case (prev, next) =>
          merger(prev, next)
        }

  def concatWithAnd(expr: Seq[Expression]): Expression =
    concat(expr) { case (a, b) =>
      And(a, b, NoLocation)
    }

  def concatWithEq(expr: Seq[Expression]): Expression =
    concat(expr) { case (a, b) =>
      Eq(a, b, NoLocation)
    }

  def newIdentifier(x: String): Identifier =
    if x.startsWith("`") && x.endsWith("`") then
      BackQuotedIdentifier(x.stripPrefix("`").stripSuffix("`"), NoLocation)
    else if x.matches("[0-9]+") then
      DigitIdentifier(x, NoLocation)
    else if !x.matches("[0-9a-zA-Z_]*") then
      // Quotations are needed with special characters to generate valid SQL
      BackQuotedIdentifier(x, NoLocation)
    else
      UnquotedIdentifier(x, NoLocation)

end Expression

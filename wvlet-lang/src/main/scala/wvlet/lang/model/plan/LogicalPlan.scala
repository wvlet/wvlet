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
package wvlet.lang.model.plan

import wvlet.lang.compiler.{Name, SourceFile}
import wvlet.lang.model.DataType.EmptyRelationType
import wvlet.lang.model.expr.NameExpr.EmptyName
import wvlet.lang.model.expr.{Attribute, AttributeList, Expression, NameExpr}
import wvlet.lang.model.{NodeLocation, RelationType, RelationTypeList, TreeNode}
import wvlet.airframe.ulid.ULID

enum PlanProperty:
  // Used for recording a Symbol defined for the tree
  case SymbolOfTree

trait LogicalPlan extends TreeNode with Product:
  // Ephemeral properties of the plan node, which will be used during compilation phases
  private var properties = Map.empty[PlanProperty, Any]

  def setProperty[A](key: PlanProperty, value: A): this.type =
    properties += key -> value
    this

  def hasProperty(key: PlanProperty): Boolean      = properties.contains(key)
  def getProperty[A](key: PlanProperty): Option[A] = properties.get(key).map(_.asInstanceOf[A])

  def isEmpty: Boolean  = false
  def nonEmpty: Boolean = !isEmpty

  def modelName: String =
    val n = this.getClass.getSimpleName
    n.stripSuffix("$")

  def pp: String = LogicalPlanPrinter.print(this)

  // True if all input attributes are resolved.
  lazy val resolved: Boolean = childExpressions.forall(_.resolved) && resolvedChildren

  def resolvedChildren: Boolean = children.forall(_.resolved)

  def unresolvedExpressions: Seq[Expression] = collectExpressions { case x: Expression =>
    !x.resolved
  }

  // def inputAttributeList: AttributeList  = AttributeList.fromSeq(inputAttributes)
  // def outputAttributeList: AttributeList = AttributeList.fromSeq(outputAttributes)

  // Input attributes (column names) of the relation
  // def inputAttributes: Seq[Attribute] = children.flatMap(_.outputAttributes)
  // Output attributes (column names) of the relation
  // def outputAttributes: Seq[Attribute]

  /**
    * All child nodes of this plan node
    *
    * @return
    */
  def children: Seq[LogicalPlan]

  // Input attributes (column names) of the relation
  def relationType: RelationType
  def inputRelationType: RelationType

  /**
    * Return child expressions associated to this LogicalPlan node
    *
    * @return
    *   child expressions of this node
    */
  def childExpressions: Seq[Expression] =
    def collectExpression(x: Any): Seq[Expression] =
      x match
        case e: Expression =>
          e :: Nil
        case p: LogicalPlan =>
          Nil
        case Some(x) =>
          collectExpression(x)
        case s: Iterable[?] =>
          s.flatMap(collectExpression _).toSeq
        case other =>
          Nil

    productIterator
      .flatMap { x =>
        collectExpression(x)
      }
      .toSeq

  def mapChildren(f: LogicalPlan => LogicalPlan): LogicalPlan =
    var changed = false

    def transformElement(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          val newExpr = e.transformPlan { case x =>
            f(x)
          }
          if !newExpr.eq(e) then
            changed = true
          newExpr
        case l: LogicalPlan =>
          val newPlan = f(l)
          if !newPlan.eq(l) then
            changed = true
          newPlan
        case Some(x) =>
          Some(transformElement(x))
        case s: Seq[?] =>
          s.map(transformElement _)
        case other: AnyRef =>
          other
        case null =>
          null

    val newArgs = productIterator.map(transformElement).toIndexedSeq
    if changed then
      copyInstance(newArgs)
    else
      this

  end mapChildren

  private def recursiveTraverse[U](f: PartialFunction[LogicalPlan, U])(arg: Any): Unit =
    def loop(v: Any): Unit =
      v match
        case e: Expression =>
          e.traversePlan(f)
        case l: LogicalPlan =>
          if f.isDefinedAt(l) then
            f.apply(l)
          l.productIterator.foreach(x => loop(x))
        case Some(x) =>
          Some(loop(x))
        case s: Seq[?] =>
          s.map(x => loop(x))
        case other: AnyRef =>
        case null          =>

    loop(arg)

  /**
    * Recursively traverse plan nodes and apply the given function to LogicalPlan nodes
    *
    * @param rule
    */
  def traverse[U](rule: PartialFunction[LogicalPlan, U]): Unit = recursiveTraverse(rule)(this)

  /**
    * Recursively traverse the child plan nodes and apply the given function to LogicalPlan nodes
    *
    * @param rule
    */
  def traverseChildren[U](rule: PartialFunction[LogicalPlan, U]): Unit = productIterator
    .foreach(child => recursiveTraverse(rule)(child))

  private def recursiveTraverseOnce[U](f: PartialFunction[LogicalPlan, U])(arg: Any): Unit =
    def loop(v: Any): Unit =
      v match
        case e: Expression =>
          e.traversePlanOnce(f)
        case l: LogicalPlan =>
          if f.isDefinedAt(l) then
            f.apply(l)
          else
            l.productIterator.foreach(x => loop(x))
        case Some(x) =>
          Some(loop(x))
        case s: Seq[?] =>
          s.map(x => loop(x))
        case other: AnyRef =>
        case null          =>

    loop(arg)

  /**
    * Recursively traverse the plan nodes until the rule matches.
    *
    * @param rule
    * @tparam U
    */
  def traverseOnce[U](rule: PartialFunction[LogicalPlan, U]): Unit =
    recursiveTraverseOnce(rule)(this)

  /**
    * Recursively traverse the child plan nodes until the rule matches.
    *
    * @param rule
    * @tparam U
    */
  def traverseChildrenOnce[U](rule: PartialFunction[LogicalPlan, U]): Unit = productIterator
    .foreach(child => recursiveTraverseOnce(rule)(child))

  /**
    * Iterate through LogicalPlans and apply matching rules for transformation. The transformation
    * will be applied to the current node as well.
    *
    * @param rule
    * @return
    */
  def transform(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan =
    val newNode: LogicalPlan = rule.applyOrElse(this, identity[LogicalPlan])
    if newNode.eq(this) then
      mapChildren(_.transform(rule))
    else
      newNode.mapChildren(_.transform(rule))

  def transformUp(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan =
    val newNode = this.mapChildren(_.transformUp(rule))
    rule.applyOrElse(newNode, identity[LogicalPlan])

  /**
    * Traverse the tree until finding the nodes matching the pattern. All nodes found from the root
    * will be transformed, and no further recursive match will occur from the transformed nodes.
    *
    * If you want to continue the transformation for the child nodes, use [[transformChildren]] or
    * [[transformChildrenOnce]] inside the rule.
    *
    * @param rule
    * @return
    */
  def transformOnce(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan =
    val newNode: LogicalPlan = rule.applyOrElse(this, identity[LogicalPlan])
    if newNode.eq(this) then
      transformChildrenOnce(rule)
    else
      // The root node was transformed
      newNode

  /**
    * Transform child node only once
    *
    * @param rule
    * @return
    */
  def transformChildren(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan =
    var changed = false

    def transformElement(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          e
        case l: LogicalPlan =>
          val newPlan = rule.applyOrElse(l, identity[LogicalPlan])
          if !newPlan.eq(l) then
            changed = true
          newPlan
        case Some(x) =>
          Some(transformElement(x))
        case s: Seq[?] =>
          s.map(transformElement _)
        case other: AnyRef =>
          other
        case null =>
          null

    val newArgs = productIterator.map(transformElement).toIndexedSeq
    if changed then
      copyInstance(newArgs)
    else
      this

  /**
    * Apply [[transformOnce]] for all child nodes.
    *
    * @param rule
    * @return
    */
  def transformChildrenOnce(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan =
    var changed = false

    def recursiveTransform(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          e
        case l: LogicalPlan =>
          val newPlan = l.transformOnce(rule)
          if !newPlan.eq(l) then
            changed = true
          newPlan
        case Some(x) =>
          Some(recursiveTransform(x))
        case s: Seq[?] =>
          s.map(recursiveTransform _)
        case other: AnyRef =>
          other
        case null =>
          null

    val newArgs = productIterator.map(recursiveTransform).toIndexedSeq
    if changed then
      copyInstance(newArgs)
    else
      this

  /**
    * Recursively transform all nested expressions
    *
    * @param rule
    * @return
    */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): LogicalPlan =
    var changed = false

    def loopOnlyPlan(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          e
        case l: LogicalPlan =>
          val newPlan = l.transformExpressions(rule)
          if l eq newPlan then
            l
          else
            changed = true
            newPlan
        case Some(x) =>
          Some(loopOnlyPlan(x))
        case s: Seq[?] =>
          s.map(loopOnlyPlan _)
        case other: AnyRef =>
          other
        case null =>
          null

    // Transform child expressions first
    val newPlan = transformChildExpressions(rule)
    val newArgs = newPlan.productIterator.map(loopOnlyPlan).toSeq
    if changed then
      copyInstance(newArgs)
    else
      newPlan

  end transformExpressions

  /**
    * Depth-first transformation of expression
    *
    * @param rule
    * @return
    */
  def transformUpExpressions(rule: PartialFunction[Expression, Expression]): LogicalPlan =
    var changed = false

    def iter(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          val newExpr = e.transformUpExpression(rule)
          if e eq newExpr then
            e
          else
            changed = true
            newExpr
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
          s.map(iter _)
        case other: AnyRef =>
          other
        case null =>
          null

    val newArgs = productIterator.map(iter).toIndexedSeq
    if changed then
      copyInstance(newArgs)
    else
      this

  end transformUpExpressions

  /**
    * Transform only child expressions
    *
    * @param rule
    * @return
    */
  def transformChildExpressions(rule: PartialFunction[Expression, Expression]): LogicalPlan =
    var changed = false

    def iterOnce(arg: Any): AnyRef =
      arg match
        case e: Expression =>
          val newExpr = rule.applyOrElse(e, identity[Expression])
          if e eq newExpr then
            e
          else
            changed = true
            newExpr
        case l: LogicalPlan =>
          l
        case Some(x) =>
          Some(iterOnce(x))
        case s: Seq[?] =>
          s.map(iterOnce _)
        case other: AnyRef =>
          other
        case null =>
          null

    val newArgs = productIterator.map(iterOnce).toIndexedSeq
    if changed then
      copyInstance(newArgs)
    else
      this

  protected def copyInstance(newArgs: Seq[AnyRef]): this.type =
    val primaryConstructor = this.getClass.getDeclaredConstructors()(0)
    val newObj             = primaryConstructor.newInstance(newArgs*)
    newObj match
      case t: TreeNode =>
        t.symbol = this.symbol
      case _ =>
    newObj.asInstanceOf[this.type]

  /**
    * List all input expressions to the plan
    *
    * @return
    */
  def inputExpressions: List[Expression] =
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

  /**
    * Collect from all input expressions and report matching expressions
    *
    * @param rule
    * @return
    */
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

    productIterator.foreach(recursiveTraverse)

end LogicalPlan

trait LeafPlan extends LogicalPlan:
  override def children: Seq[LogicalPlan]      = Nil
  override def inputRelationType: RelationType = EmptyRelationType

trait UnaryPlan extends LogicalPlan:
  def child: LogicalPlan
  override def children: Seq[LogicalPlan] = child :: Nil
  override def inputRelationType: RelationType =
    child match
      case r: Relation =>
        r.relationType
      case _ =>
        EmptyRelationType

trait BinaryPlan extends LogicalPlan:
  def left: LogicalPlan
  def right: LogicalPlan
  override def children: Seq[LogicalPlan] = Seq(left, right)

object LogicalPlan:
  val empty = PackageDef(name = EmptyName, statements = Nil, nodeLocation = None)

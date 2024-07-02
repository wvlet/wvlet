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
package wvlet.lang.compiler.ast

import wvlet.lang.compiler.core.{Name, Type}
import wvlet.lang.compiler.core.Name.{NoName, TermName, TypeName}

abstract class Tree:
  protected var sourcePosition: SourcePosition   = SourcePosition.NoPosition
  private var _span: Span                        = Span.NoSpan
  private var _tpe: Type                         = Type.Unknown
  private var tokenDecorator: Set[TreeDecorator] = Set.empty

  def withSpan(span: Span): this.type =
    if _span == span then this
    else
      if !_span.exists then if span.exists then _span = span
      this

  def isDecoratedWith(decorator: TreeDecorator): Boolean =
    tokenDecorator.contains(decorator)

  def addDecorator(decorator: TreeDecorator): this.type =
    tokenDecorator += decorator
    this

  def withType(t: Type): this.type = ???

  def nodeName: String
  def toJson: String = ???

abstract class Plan       extends Tree
abstract class Expression extends Tree

enum TreeDecorator:
  case SingleQuoted
  case DoubleQuoted
  case TripleQuoted
  case BackQuoted
  case CommaSeparated
  case IndentedBlock
  case ApplyWithoutParen
  case RefDot
  case RefSpace

object Tree:
  import TreeDecorator.*

  def fromJson(json: String): Tree = ???

  case object EmptyTree extends Tree

  // Relational operator
  abstract class Relation(input: Tree, name: Name, args: List[Expression]) extends Plan:
    def isCommaSeparated: Boolean = !isIndentedBlock
    def isIndentedBlock: Boolean  = isDecoratedWith(IndentedBlock)

  case class Join(joinType: JoinType, left: Relation, right: Relation, joinCriteria: JoinCriteria)
      extends Relation(left, TermName("join"), List(joinType, joinCriteria)):

    override def nodeName: String = "JoinOp"

  enum JoinType(name: String) extends Expression:
    // Join for reporting only matched rows
    case InnerJoin extends JoinType("inner_join")
    // Join for preserving left table
    case LeftJoin extends JoinType("left_join")
    // Join for preserving right table
    case RightJoin extends JoinType("right_join")
    // Join for preserving both tables
    case FullJoin extends JoinType("full_join")
    // Join for cartesian product
    case CrossJoin extends JoinType("cross_join")

  sealed trait JoinCriteria                       extends Expression
  case class JoinOn(expr: Expression)             extends JoinCriteria
  case class JoinUsing(columns: List[Identifier]) extends JoinCriteria

  // Code block
  case class Block(statements: Seq[Tree], lastExpr: Tree) extends Tree

  // Import statement
  case class Import(prefix: Symbol, selectors: List[ImportSelector]) extends Tree
  case class ImportSelector(imported: Identifier, renamed: Name = NoName) extends Tree:
    def isWildcard: Boolean = imported.name == Name.WILDCARD

  // Function arg application
  case class Apply(func: Tree, args: List[Tree]) extends Tree:
    def isCommaSeparatedArgs: Boolean = !isIndentedBlockArgs
    def isIndentedBlockArgs: Boolean  = isDecoratedWith(IndentedBlock)
    def hasArgParen: Boolean          = !isDecoratedWith(ApplyWithoutParen)

  // Follow (expr).name reference
  case class Ref(expr: Tree, name: Name) extends Tree

  abstract case class Identifier(name: Name) extends Tree:
    def isBackquoted: Boolean = isDecoratedWith(TreeDecorator.BackQuoted)

  class Literal(stringRepr: String, literalType: LiteralType) extends Tree
  enum LiteralType:
    case NullLiteral
    case BooleanLiteral
    case IntLiteral
    case LongLiteral
    case FloatLiteral
    case DoubleLiteral
    case StringLiteral
    case DecimalLiteral
    case BinaryLiteral

  case class DefDef(name: TermName, args: List[Tree], retType: Tree, body: Tree) extends Tree
  case class ValDef(name: TermName, tpe: Tree, body: Tree)                       extends Tree
  case class TypeDef(name: TypeName, body: Tree)                                 extends Tree

  case class PackageDef(name: Name, statements: List[Tree]) extends Tree

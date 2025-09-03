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
package wvlet.lang.model

import wvlet.lang.api.{LinePosition, SourceLocation, Span, StatusCode}
import wvlet.lang.compiler.{CompilationUnit, Context, SourceFile, Symbol}
import wvlet.lang.compiler.ContextUtil.*
import wvlet.lang.compiler.parser.TokenData
import wvlet.lang.model.plan.LogicalPlan
import wvlet.log.LogSupport

/**
  * A base class for LogicalPlan and Expression
  */
trait TreeNode extends TreeNodeCompat

trait SyntaxTreeNode extends TreeNode with Product with LogSupport:
  private var _symbol: Symbol                  = Symbol.NoSymbol
  private var _comment: List[TokenData[?]]     = Nil
  private var _postComment: List[TokenData[?]] = Nil

  def linePosition(using ctx: Context): LinePosition = ctx.sourceLocationAt(span).position

  def endLinePosition(using ctx: Context): LinePosition =
    ctx.compilationUnit.sourceFile.sourceLocationAt(span.end).position

  def childNodes: List[SyntaxTreeNode] =
    val l = List.newBuilder[SyntaxTreeNode]
    def loop(x: Any): Unit =
      x match
        case n: SyntaxTreeNode =>
          l += n
        case xs: Seq[?] =>
          xs.foreach(loop)
        case o: Option[?] =>
          o.foreach(loop)
        case i: Iterator[?] =>
          i.foreach(loop)
        case _ =>
    loop(this.productIterator)
    l.result()

  def collectAllNodes: List[SyntaxTreeNode] =
    val lst = List.newBuilder[SyntaxTreeNode]
    lst += this
    this
      .childNodes
      .foreach { n =>
        lst ++= n.collectAllNodes
      }
    lst.result()

  def copyMetadatFrom(t: SyntaxTreeNode): Unit =
    // Copy symbol, comment
    _symbol = t._symbol
    _comment = t._comment
    _postComment = t._postComment

  def symbol: Symbol            = _symbol
  def symbol_=(s: Symbol): Unit = _symbol = s

  def comments: List[TokenData[?]]     = _comment
  def postComments: List[TokenData[?]] = _postComment

  def withComment(d: TokenData[?]): this.type =
    _comment = d :: _comment
    this

  def withPostComment(d: TokenData[?]): this.type =
    _postComment = d :: _postComment
    this

  protected def copyInstance(newArgs: Seq[AnyRef]): this.type =
    // Using non-JVM reflection to support Scala.js/Scala Native
    try
      val args = newArgs.map { (x: Any) =>
        x match
          case s: Span =>
            // Span can be a plain Long type due to Scala's internal optimization
            s.coordinate
          case other =>
            other
      }
      val newObj = getSingletonObject.getOrElse(newInstance(args*))
      newObj match
        case t: SyntaxTreeNode =>
          if this.symbol.tree != null then
            // Update the tree reference with the rewritten tree
            this.symbol.tree = t
          // Copy metadata to preserve symbol and comments
          t.copyMetadatFrom(this)
        case _ =>
      newObj.asInstanceOf[this.type]
    catch
      case e: IllegalArgumentException =>
        val details =
          e.getCause match
            case ce: ClassCastException =>
              s"${e.getMessage}: ${ce.getMessage}"
            case _ =>
              e.getMessage

        throw StatusCode
          .COMPILATION_FAILURE
          .newException(
            s"Failed to create ${nodeName} node: ${details}\n[args]${newArgs.mkString(
                "\n - ",
                "\n - ",
                ""
              )}",
            e
          )

  /**
    * @return
    *   the code location in the SQL text if available
    */
  def span: Span
  def sourceLocation(using ctx: Context): SourceLocation = ctx.sourceLocationAt(span)

  def sourceLocationOfCompilationUnit(using cu: CompilationUnit): SourceLocation = cu
    .sourceLocationAt(span)

  def locationString(using ctx: Context): String = sourceLocation(using ctx).locationString

  def nodeName: String =
    val n = this.getClass.getSimpleName
    n.stripSuffix("$")

end SyntaxTreeNode

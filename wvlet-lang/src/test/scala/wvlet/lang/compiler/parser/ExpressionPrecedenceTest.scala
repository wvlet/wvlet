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
package wvlet.lang.compiler.parser

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.expr.*
import wvlet.uni.test.UniTest

/**
  * Operator precedence tests for both parsers: OR < AND < NOT < comparison < additive <
  * multiplicative < unary, with binary operators associating to the left
  */
class ExpressionPrecedenceTest extends UniTest:

  private def sqlExpr(s: String): Expression = SqlParser(CompilationUnit.fromSqlString(s))
    .expression()

  private def wvExpr(s: String): Expression = WvletParser(CompilationUnit.fromWvletString(s))
    .expression()

  private def checkPrecedence(parse: String => Expression): Unit =
    // multiplicative binds tighter than additive
    parse("a + b * c") shouldMatch {
      case ArithmeticBinaryExpr(
            BinaryExprType.Add,
            _,
            ArithmeticBinaryExpr(BinaryExprType.Multiply, _, _, _),
            _
          ) =>
    }
    parse("a * b + c") shouldMatch {
      case ArithmeticBinaryExpr(
            BinaryExprType.Add,
            ArithmeticBinaryExpr(BinaryExprType.Multiply, _, _, _),
            _,
            _
          ) =>
    }
    // additive is left-associative: a - b + c = (a - b) + c
    parse("a - b + c") shouldMatch {
      case ArithmeticBinaryExpr(
            BinaryExprType.Add,
            ArithmeticBinaryExpr(BinaryExprType.Subtract, _, _, _),
            _,
            _
          ) =>
    }
    // arithmetic binds tighter than comparison: a / b < c = (a / b) < c
    parse("a / b < c") shouldMatch {
      case LessThan(ArithmeticBinaryExpr(BinaryExprType.Divide, _, _, _), _, _) =>
    }
    parse("a < b + c") shouldMatch {
      case LessThan(_, ArithmeticBinaryExpr(BinaryExprType.Add, _, _, _), _) =>
    }
    // AND binds tighter than OR: a and b or c = (a and b) or c
    parse("a and b or c") shouldMatch { case Or(And(_, _, _), _, _) =>
    }
    parse("a or b and c") shouldMatch { case Or(_, And(_, _, _), _) =>
    }
    // comparison binds tighter than AND: a < b and c = (a < b) and c
    parse("a < b and c > d") shouldMatch { case And(LessThan(_, _, _), GreaterThan(_, _, _), _) =>
    }
    // NOT binds tighter than AND: not a and b = (not a) and b
    parse("not a and b") shouldMatch { case And(Not(_, _), _, _) =>
    }
    // unary minus binds tighter than multiplication: -a * b = (-a) * b
    parse("-a * b") shouldMatch {
      case ArithmeticBinaryExpr(
            BinaryExprType.Multiply,
            ArithmeticUnaryExpr(Sign.Negative, _, _),
            _,
            _
          ) =>
    }
    // between operands stop at the additive level
    parse("a between b + 1 and c + 2") shouldMatch {
      case Between(
            _,
            ArithmeticBinaryExpr(BinaryExprType.Add, _, _, _),
            ArithmeticBinaryExpr(BinaryExprType.Add, _, _, _),
            _
          ) =>
    }
  end checkPrecedence

  test("should parse SQL operators with standard precedence") {
    checkPrecedence(sqlExpr)
  }

  test("should parse Wvlet operators with standard precedence") {
    checkPrecedence(wvExpr)
  }

end ExpressionPrecedenceTest

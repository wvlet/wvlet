package wvlet.lang.compiler.query

import wvlet.lang.api.LinePosition
import wvlet.lang.api.v1.query.QuerySelection
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.expr.NameExpr
import wvlet.lang.model.plan.*

object QuerySelector:

  def selectQuery(
      unit: CompilationUnit,
      linePosition: LinePosition,
      selection: QuerySelection
  ): LogicalPlan =
    val targetOffset =
      if linePosition.isEmpty then
        unit.sourceFile.length - 1
      else
        unit.sourceFile.offsetAt(linePosition)

    selectQuery(unit.resolvedPlan, targetOffset, selection)

  def selectQuery(
      inputPlan: LogicalPlan,
      targetOffset: Int,
      selection: QuerySelection
  ): LogicalPlan =

    def findTargetStatement(plan: LogicalPlan): Option[TopLevelStatement] = plan
      .children
      .collectFirst {
        // Find the top-level statement that contains the target offset
        case l: TopLevelStatement if l.span.containsInclusive(targetOffset) =>
          l
      }

    def extractSubQuery(plan: LogicalPlan): Option[Relation] =
      // Find the smallest subquery that contains the target offset
      var subQuery: Option[Relation] = None
      plan.traverse {
        case r: Relation
            if r.span.containsInclusive(targetOffset) &&
              (subQuery.isEmpty || subQuery.exists(x => r.span.size < x.span.size)) =>
          subQuery = Some(r)
      }
      subQuery

    selection match
      case QuerySelection.Single =>
        findTargetStatement(inputPlan).getOrElse(inputPlan)
      case QuerySelection.Subquery =>
        findTargetStatement(inputPlan)
          .flatMap { stmt =>
            extractSubQuery(stmt).map { r =>
              Query(r, stmt.span.extendTo(r.span))
            }
          }
          .getOrElse(inputPlan)
      case QuerySelection.Describe =>
        findTargetStatement(inputPlan)
          .flatMap { stmt =>
            extractSubQuery(stmt).map { r =>
              Describe(r, stmt.span.extendTo(r.span))
            }
          }
          .getOrElse(inputPlan)
      case QuerySelection.AllBefore =>
        inputPlan.match
          case p: PackageDef =>
            val stmts = List.newBuilder[TopLevelStatement]
            p.traverseChildren {
              case t: TopLevelStatement
                  if t.span.end <= targetOffset || t.span.containsInclusive(targetOffset) =>
                stmts += t
            }
            p.copy(statements = stmts.result())
          case _ =>
            inputPlan
      case QuerySelection.All =>
        inputPlan
    end match

  end selectQuery

end QuerySelector

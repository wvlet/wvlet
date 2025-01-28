package wvlet.lang.compiler.analyzer

import wvlet.lang.model.plan.{LogicalPlan, PackageDef, Query, WithQuery}
import wvlet.lang.compiler.Context
import wvlet.lang.model.expr.Expression

import scala.collection.immutable.ListMap

/**
  * Mappings from LogicalPlan and its rank (e.g., syntax order, dataflow order)
  */
type LogicalPlanRankTable = ListMap[LogicalPlan, Int]
object LogicalPlanRankTable:
  val empty: LogicalPlanRankTable = ListMap.empty[LogicalPlan, Int]

object LogicalPlanRank:

  /**
    * Compute the syntax rank (based on line position in the source code) table of the LogicalPlan
    * nodes in the given plan
    * @param l
    * @param ctx
    * @return
    */
  def syntaxRank(l: LogicalPlan)(using ctx: Context): LogicalPlanRankTable =
    val nodes = List.newBuilder[LogicalPlan]

    nodes += l
    l.traverse { case c: LogicalPlan =>
      nodes += c
    }

    ListMap.from(
      nodes
        .result()
        .sortBy(n => n.sourceLocation.position)
        .zipWithIndex
        .map: (n, rank) =>
          n -> (rank + 1)
    )

  /**
    * Compute the dataflow rank table of the LogicalPlan nodes in the given plan
    * @param l
    * @return
    */
  def dataflowRank(l: LogicalPlan): LogicalPlanRankTable =
    val postOrder = List.newBuilder[LogicalPlan]

    def dfs(p: LogicalPlan): Unit =
      def traverseChildren(plan: LogicalPlan): Unit = plan
        .childNodes
        .foreach {
          case c: LogicalPlan =>
            dfs(c)
          case e: Expression =>
            e.traversePlanOnce { case n: LogicalPlan =>
              dfs(n)
            }
          case _ =>
        }

      p match
        case q: Query =>
          dfs(q.body)
        case p: PackageDef =>
          p.statements.foreach(dfs)
        case w: WithQuery =>
          w.queryDefs
            .foreach { n =>
              // Skip AliasedRelation
              dfs(n.child)
            }
          dfs(w.queryBody)
        case plan: LogicalPlan =>
          traverseChildren(plan)
          postOrder += plan
    end dfs

    dfs(l)

    ListMap.from(
      postOrder
        .result()
        .zipWithIndex
        .map: (n, i) =>
          n -> (i + 1)
    )
  end dataflowRank

end LogicalPlanRank

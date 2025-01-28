package wvlet.lang.compiler.analyzer

import wvlet.lang.model.plan.{LogicalPlan, PackageDef, Query, WithQuery}
import wvlet.lang.compiler.Context
import wvlet.lang.model.expr.Expression
import wvlet.log.LogSupport

import scala.collection.immutable.ListMap

/**
  * Mappings from LogicalPlan and its rank (e.g., syntax order, dataflow order)
  */
type LogicalPlanRankTable = Map[LogicalPlan, Int]
object LogicalPlanRankTable:
  val empty: LogicalPlanRankTable = Map.empty[LogicalPlan, Int]

object LogicalPlanRank extends LogSupport:

  private def collectNodesInPostOrder(l: LogicalPlan): List[LogicalPlan] =
    val nodes = List.newBuilder[LogicalPlan]
    l.dfs {
      case q: Query =>
        q
      case p: PackageDef =>
        p
      case w: WithQuery =>
        w
      case plan: LogicalPlan =>
        nodes += plan
    }
    nodes.result()

  /**
    * Compute the syntax rank (based on line position in the source code) table of the LogicalPlan
    * nodes in the given plan
    * @param l
    * @param ctx
    * @return
    */
  def syntaxRank(l: LogicalPlan)(using ctx: Context): LogicalPlanRankTable =
    val nodes = collectNodesInPostOrder(l)
    Map.from(
      nodes
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
    val postOrder = collectNodesInPostOrder(l)
    Map.from(
      postOrder
        .zipWithIndex
        .map: (n, i) =>
          n -> (i + 1)
    )
  end dataflowRank

  def syntaxReadability(l: LogicalPlan)(using ctx: Context): Unit =
    val syntaxRanks   = syntaxRank(l)
    val dataflowRanks = dataflowRank(l)

    // Compute eye-movement distance
    syntaxRanks.toSeq.sortBy(c => dataflowRanks(c._1)) map { (plan, syntaxRank) =>
      val dataflowRank = dataflowRanks(plan)
      val distance     = math.abs(syntaxRank - dataflowRank)
      info(f"${dataflowRank}%2s) [${syntaxRank}%2s] ${plan.nodeName}%16s dist:${distance}")
    }

    ()

end LogicalPlanRank

package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.Context
import wvlet.lang.model.plan.*
import wvlet.log.LogSupport

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
        // Query node is just a place holder
        q
      case p: PackageDef =>
        // Ignore package definition
        p
      case w: WithQuery =>
        // Ignore with definition as it's always preceeds the main query
        w
      case b: BracedRelation =>
        // Ignore the brace only node
        b
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

  case class ReadabilityScore(
      l: LogicalPlan,
      eyeMovement: Int,
      lineMovement: Int,
      inversionCount: Int,
      syntaxRankDist: Int,
      numPlanNodes: Int
  ):
    override def toString: String =
      s"eyeMovement:${eyeMovement}, lineMovement:${lineMovement}, inversions:${inversionCount}, syntaxRankDist:${syntaxRankDist}"

  def syntaxReadability(l: LogicalPlan)(using ctx: Context): ReadabilityScore =
    val syntaxRanks = syntaxRank(l)
    // Sort nodes in the order of dataflow
    val dataflowRanks = dataflowRank(l).toSeq.sortBy(_._2)

    var eyeMovement: Int    = 0
    var lineMovement: Int   = 0
    var syntaxRankDiff: Int = 0
    var inversionCount: Int = 0

    // Compute eye-movement distance
    dataflowRanks.reduce { (prev, current) =>
      val dataflowRank1 = prev._2
      val dataflowRank2 = current._2

      val syntaxRank1 = syntaxRanks(prev._1)
      val syntaxRank2 = syntaxRanks(current._1)
      syntaxRankDiff += (syntaxRank2 - syntaxRank1).abs

      // end line -> next start line
      val line1 = prev._1.endLinePosition.line
      val line2 = current._1.linePosition.line
      lineMovement += (line2 - line1).abs

      // end offset -> next start offset
      val offset1             = prev._1.span.end
      val offset2             = current._1.span.start
      val offsetMovement: Int = (offset2 - offset1).abs
      eyeMovement += offsetMovement

      trace(
        s"${dataflowRank1}) [${syntaxRank1}] ${prev
            ._1
            .nodeName} -> ${dataflowRank2}) [${syntaxRank2}] ${current
            ._1
            .nodeName}, dist: ${offsetMovement}, line:${line1} -> ${line2}"
      )

      // Check if the syntax rank is inverted
      if syntaxRank1 > syntaxRank2 then
        inversionCount += 1

      // pass the current node to the next iteration
      current
    }

    ReadabilityScore(
      l,
      eyeMovement,
      lineMovement,
      inversionCount,
      syntaxRankDiff,
      numPlanNodes = syntaxRanks.size
    )
  end syntaxReadability

end LogicalPlanRank

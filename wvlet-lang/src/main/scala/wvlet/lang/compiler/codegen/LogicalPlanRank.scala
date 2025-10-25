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
      // Query node is a placeholder
      case p: PackageDef =>
      // Ignore package definition
      case w: WithQuery =>
      // Ignore with definition as it's always preceeds the main query
      case b: BracedRelation =>
      // Ignore the brace only node
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
      // join or set operation count
      joinCount: Int,
      syntaxRankDist: Int,
      numPlanNodes: Int
  ):
    def maxSyntaxRankDist: Int = numPlanNodes * (numPlanNodes - 1) / 2
    def maxInversionCount: Int = numPlanNodes - 1

    def inversionScore: Double  = 1.0 - (inversionCount.toDouble / maxInversionCount)
    def syntaxRankScore: Double =
      1.0 - ((syntaxRankDist.toDouble - numPlanNodes + 1) / (maxSyntaxRankDist - numPlanNodes + 1))

    def normalizedScore: Double = (inversionScore + syntaxRankScore) / 2.0

    def pp: String =
      f"score: ${normalizedScore}%.2f, inversion: ${inversionCount} (${joinCount} joins), syntax rank dist: ${syntaxRankDist}, eye movement: ${eyeMovement}, line movement: ${lineMovement}"

  def syntaxReadability(l: LogicalPlan)(using ctx: Context): ReadabilityScore =
    def isConnectNode(p: LogicalPlan): Boolean =
      p match
        case _: Join | _: SetOperation =>
          true
        case _ =>
          false

    val syntaxRanks = syntaxRank(l)
    // Sort nodes in the order of dataflow
    val dataflowRanks = dataflowRank(l).toSeq.sortBy(_._2)

    // Init with the first eye movement to the leaf node
    var eyeMovement: Int    = dataflowRanks(0)._1.span.start
    var lineMovement: Int   = dataflowRanks(0)._1.linePosition.line
    var syntaxRankDiff: Int = syntaxRanks(dataflowRanks(0)._1)
    var inversionCount: Int = 0
    var joinCount: Int      =
      if isConnectNode(dataflowRanks(0)._1) then
        1
      else
        0

    // Compute eye-movement distance
    dataflowRanks.reduce { (prev, current) =>
      val dataflowRank1 = prev._2
      val dataflowRank2 = current._2

      // end line -> next start line
      val line1 = prev._1.endLinePosition.line
      val line2 = current._1.linePosition.line
      lineMovement += (line2 - line1).abs

      // Compute the syntax rank distance to remove the effect of text formatting differences
      val syntaxRank1 = syntaxRanks(prev._1)
      val syntaxRank2 = syntaxRanks(current._1)
      syntaxRankDiff += (syntaxRank2 - syntaxRank1).abs

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
      if line1 != line2 && syntaxRank1 > syntaxRank2 then
        inversionCount += 1

      // Check if the current node is a join or set operation node
      if isConnectNode(current._1) then
        joinCount += 1

      // pass the current node to the next iteration
      current
    }

    ReadabilityScore(
      l,
      eyeMovement,
      lineMovement,
      inversionCount,
      joinCount,
      syntaxRankDiff,
      // +1 for the beggining of the query
      numPlanNodes = syntaxRanks.size + 1
    )
  end syntaxReadability

end LogicalPlanRank

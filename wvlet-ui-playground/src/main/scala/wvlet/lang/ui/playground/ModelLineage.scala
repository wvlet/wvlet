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
package wvlet.lang.ui.playground

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.pipeline.ModelBuildPlanner
import wvlet.lang.compiler.pipeline.ModelBuildSpec
import wvlet.lang.compiler.pipeline.ModelOrder
import wvlet.lang.model.DataType
import wvlet.lang.model.plan.*

import scala.util.control.NonFatal

/** What a node of the lineage graph stands for */
enum LineageKind(val label: String):
  /** A table of the catalog (the generated TPC-H tables) */
  case Source extends LineageKind("table")

  /** Inline rows defined with `val t(a, b) = [...]`, like a dbt seed */
  case Seed extends LineageKind("seed")

  /** A `model` definition */
  case Model extends LineageKind("model")

  /** A top-level query of the file */
  case Query extends LineageKind("query")

/**
  * A node of the lineage graph
  *
  * @param id
  *   unique id in the graph
  * @param label
  *   the name shown in the node
  * @param detail
  *   the second line: the materialization of a model (and its target schema), or the kind of the
  *   node
  * @param line
  *   the 1-origin source line that defines the node, if it is defined in the file
  * @param target
  *   the relation `wvlet build` persists the node to (a model with materialized: 'view' or 'table')
  */
case class LineageNode(
    id: String,
    label: String,
    kind: LineageKind,
    detail: String,
    line: Option[Int] = None,
    target: Option[String] = None
):
  def materialized: Boolean = target.nonEmpty

/** An edge from a relation to a model or query that reads it */
case class LineageEdge(from: String, to: String)

/**
  * The data lineage of a file: tables and seeds flow into models, and models into other models and
  * queries. It shows the same dependency graph `wvlet build` derives from model references,
  * including ephemeral models and the queries reading the models.
  *
  * @param buildOrder
  *   the materialized models in the order `wvlet build` builds them
  * @param buildError
  *   why the file can't be built (e.g., a dependency cycle), if any
  */
case class ModelLineage(
    nodes: Seq[LineageNode] = Nil,
    edges: Seq[LineageEdge] = Nil,
    buildOrder: Seq[String] = Nil,
    buildError: Option[String] = None
):
  def isEmpty: Boolean = nodes.isEmpty

  /** True when the file defines at least one model */
  def hasModels: Boolean = nodes.exists(_.kind == LineageKind.Model)

  private lazy val ups: Map[String, List[String]] = edges
    .groupBy(_.to)
    .map((k, es) => k -> es.map(_.from).toList)

  private lazy val downs: Map[String, List[String]] = edges
    .groupBy(_.from)
    .map((k, es) => k -> es.map(_.to).toList)

  def upstreamsOf(id: String): List[String]   = ups.getOrElse(id, Nil)
  def downstreamsOf(id: String): List[String] = downs.getOrElse(id, Nil)

  /** The node, everything it reads, and everything that reads it */
  def related(id: String): Set[String] =
    ModelOrder.closure(id, upstreamsOf) ++ ModelOrder.closure(id, downstreamsOf)

  /**
    * The row of each node in a top-down layout: the length of the longest path from a node that
    * reads nothing. Nodes on a cycle (a compile error) stay in the row first assigned
    */
  lazy val depth: Map[String, Int] =
    val d = scala.collection.mutable.Map.empty[String, Int]
    def loop(id: String, visiting: Set[String]): Int = d.getOrElse(
      id, {
        val ds = upstreamsOf(id).filterNot(visiting.contains).map(u => loop(u, visiting + id) + 1)
        val v  = (0 :: ds).max
        d(id) = v
        v
      }
    )
    nodes.foreach(n => loop(n.id, Set.empty))
    d.toMap

  /**
    * The nodes of each row, top to bottom. Each row is ordered by the mean position of the nodes it
    * reads (the barycenter heuristic), so edges cross less; the first row is then ordered by the
    * mean position of its readers. Ties keep the source order, so the graph follows the file
    */
  lazy val layers: Seq[Seq[LineageNode]] =
    if nodes.isEmpty then
      Nil
    else
      val maxDepth = depth.values.max
      val initial  = (0 to maxDepth).map { r =>
        nodes.filter(n => depth(n.id) == r).sortBy(n => (n.line.getOrElse(-1), n.label))
      }
      // Rows are drawn centered, so a node's position is its offset from the middle of its row
      def positions(rows: Seq[Seq[LineageNode]]): Map[String, Double] =
        rows
          .flatMap(row => row.zipWithIndex.map((n, i) => n.id -> (i - (row.size - 1) / 2.0)))
          .toMap
      def byMean(
          row: Seq[LineageNode],
          pos: Map[String, Double],
          neighbors: String => List[String]
      ): Seq[LineageNode] =
        val own = positions(Seq(row))
        row
          .zipWithIndex
          .sortBy { (n, i) =>
            val ps   = neighbors(n.id).flatMap(pos.get)
            val mean =
              if ps.isEmpty then
                own(n.id)
              else
                ps.sum / ps.size
            (mean, i)
          }
          .map(_._1)
      // Downward: order each row by the nodes it reads
      val down =
        initial
          .tail
          .foldLeft(Vector(initial.head)) { (done, row) =>
            done :+ byMean(row, positions(done), upstreamsOf)
          }
      // Upward for the first row: place tables and seeds above the nodes that read them
      if down.size > 1 then
        byMean(down.head, positions(down.tail), downstreamsOf) +: down.tail
      else
        down

end ModelLineage

object ModelLineage:

  /** The 1-origin source line where a plan node starts, if it has a position */
  def lineOf(unit: CompilationUnit, p: LogicalPlan): Option[Int] =
    val s = p.span
    if s.nonEmpty && s.start >= 0 then
      Some(unit.sourceFile.offsetToLine(s.start) + 1)
    else
      None

  private def modelId(name: String): String = s"model:${name}"
  private def seedId(name: String): String  = s"seed:${name}"
  private def tableId(name: String): String = s"table:${name}"

  /**
    * The lineage of a compiled unit. References are read from the unresolved plan, which still
    * names the seeds (`val` tables) that the resolved plan has inlined as values
    *
    * @param tableNames
    *   tables of the catalog: names read by the file that are none of its models or seeds (e.g., a
    *   `with` alias) are left out
    */
  def of(unit: CompilationUnit, tableNames: Set[String])(using ctx: Context): ModelLineage =
    val src                                 = unit.sourceFile
    def lineOf(p: LogicalPlan): Option[Int] = ModelLineage.lineOf(unit, p)

    val statements =
      unit.unresolvedPlan match
        case p: PackageDef =>
          p.statements
        case other =>
          List(other)

    val models = statements.collect { case m: ModelDef =>
      m
    }
    val modelNames = models.map(_.name.name).toSet
    // Only `val t(a, b) = [...]` defines a table; a scalar val is read as a value, not a relation
    val seeds = statements.collect {
      case v: ValDef if v.dataType.isInstanceOf[DataType.SchemaType] =>
        v
    }
    val seedNames = seeds.map(_.name.name).toSet

    // The relations a statement reads: models, seeds, and catalog tables
    def readsOf(p: LogicalPlan): List[String] =
      val refs                    = List.newBuilder[String]
      def add(name: String): Unit =
        val leaf = name.split('.').last
        if modelNames.contains(leaf) then
          refs += modelId(leaf)
        else if seedNames.contains(leaf) then
          refs += seedId(leaf)
        else if tableNames.contains(leaf) then
          refs += tableId(leaf)
      p.traverse {
        case r: TableRef =>
          add(r.name.fullName)
        case f: TableFunctionCall =>
          add(f.name.fullName)
      }
      refs.result().distinct

    val nodes = List.newBuilder[LineageNode]
    val edges = List.newBuilder[LineageEdge]
    val used  = scala.collection.mutable.LinkedHashSet.empty[String]

    def connect(to: String, reads: List[String]): Unit = reads.foreach { r =>
      used += r
      edges += LineageEdge(r, to)
    }

    models.foreach { m =>
      val id   = modelId(m.name.name)
      val spec =
        try
          Some(ModelBuildSpec.of(m))
        catch
          case NonFatal(_) =>
            None
      val params =
        if m.params.isEmpty then
          ""
        else
          m.params.map(_.name.name).mkString("(", ", ", ")")
      val detail =
        spec match
          case Some(s) if s.isMaterialized =>
            s"${s.materialization.configName}${s.schema.map(sc => s" in ${sc}").getOrElse("")}"
          case _ if m.params.nonEmpty =>
            "ephemeral · parameterized"
          case _ =>
            "ephemeral"
      nodes +=
        LineageNode(
          id,
          s"${m.name.name}${params}",
          LineageKind.Model,
          detail,
          lineOf(m),
          target = spec.filter(_.isMaterialized).map(_.targetName(m.name.name))
        )
      connect(id, readsOf(m.child))
    }

    // Top-level queries: label each with its first line, e.g. `from dim_customers`
    var queryCount = 0
    statements.foreach {
      case q: (Query | TestRelation) =>
        val reads = readsOf(q)
        if reads.nonEmpty then
          queryCount += 1
          val id   = s"query:${queryCount}"
          val line = lineOf(q)
          val text = line.map(l => src.sourceLine(l).trim).getOrElse("query")
          nodes += LineageNode(id, text, LineageKind.Query, "query", line)
          connect(id, reads)
      case _ =>
    }

    // Tables and seeds that something reads
    seeds.foreach { v =>
      val id = seedId(v.name.name)
      if used.contains(id) then
        nodes += LineageNode(id, v.name.name, LineageKind.Seed, "inline values", lineOf(v))
    }
    used
      .filter(_.startsWith("table:"))
      .toSeq
      .sorted
      .foreach { id =>
        nodes += LineageNode(id, id.stripPrefix("table:"), LineageKind.Source, "source")
      }

    // The order `wvlet build` builds the materialized models in, from the resolved models
    val resolvedModels =
      unit.resolvedPlan match
        case p: PackageDef =>
          p.statements
            .collect { case m: ModelDef =>
              m
            }
        case _ =>
          Nil
    val (order, buildError) =
      try
        val g = ModelBuildPlanner.graph(resolvedModels)
        (g.buildOrder(g.models).map(_.name), None)
      catch
        case NonFatal(e) =>
          (Nil, Some(Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))

    ModelLineage(nodes.result(), edges.result().distinct, order, buildError)

  end of

end ModelLineage

package wvlet.lang.compiler.analyzer

import wvlet.lang.compiler.{CompilationUnit, Context, Phase, Symbol}
import wvlet.lang.model.plan.{LogicalPlan, ModelDef, ModelScan, PackageDef, QueryStatement}

import scala.collection.immutable.ListMap

class ModelDependencyAnalyzer extends Phase("model-dependency-analyzer"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    given Context = context

    val dependencies = ListMap.newBuilder[Symbol, List[Symbol]]
    if context.isContextCompilationUnit then
      unit
        .resolvedPlan
        .traverseOnce { case p: PackageDef =>
          p.traverseChildren {
            case q: QueryStatement =>
              dependencies ++= analyzeDependency(q)
            case m: ModelDef =>
              dependencies ++= analyzeDependency(m)
          }
        }

    val dag = DependencyDAG(dependencies.result())
    if !dag.isEmpty then
      debug(s"[dependency graph]\n${dag.printGraph}")
    unit

  private def analyzeDependency(l: LogicalPlan)(using ctx: Context): ListMap[Symbol, List[Symbol]] =
    val edges = ListMap.newBuilder[Symbol, List[Symbol]]
    l.traverse { case m: ModelScan =>
      val dependentModels = List.newBuilder[Symbol]
      ctx.findTermSymbolByName(m.name.name) match
        case Some(s) =>
          dependentModels += s
        case None =>
          warn(s"Model ${m.name} not found in the context")
      val deps = dependentModels.result()
      if deps.nonEmpty then
        edges += l.symbol -> deps
    }
    edges.result()

end ModelDependencyAnalyzer

case class DependencyDAG(nodes: ListMap[Symbol, List[Symbol]]):
  def isEmpty: Boolean  = nodes.isEmpty
  def nonEmpty: Boolean = !isEmpty

  def printGraph(using Context): String =
    val b = new StringBuilder
    nodes.foreach { (s, deps) =>
      b.append(
        s"${s} (${s.tree.sourceLocation}) -> ${deps.map(d => s"${d} (${d.tree.sourceLocation})").mkString(", ")}\n"
      )
    }
    b.result()

  override def toString: String =
    val b = new StringBuilder
    nodes.foreach { (s, deps) =>
      b.append(s"${s} -> ${deps.mkString(", ")}\n")
    }
    b.result()

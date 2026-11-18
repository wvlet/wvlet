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
package wvlet.lang.compiler.pipeline

import wvlet.lang.api.Span
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.analyzer.TableBindings
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/** How `wvlet build` persists a model */
enum Materialization(val configName: String):
  /** Inlined into the queries that reference it (the default, and the behavior of plain models) */
  case Ephemeral extends Materialization("ephemeral")

  /** Persisted as a view (create-or-replace) */
  case View extends Materialization("view")

  /** Persisted as a table, replaced on every build */
  case Table extends Materialization("table")

object Materialization:
  def fromName(name: String): Option[Materialization] = values.find(_.configName == name)

/**
  * The validated `with { }` config of a model
  *
  * @param materialization
  *   How the model is persisted
  * @param schema
  *   Target schema (`schema:`), or None for the current schema of the profile
  * @param alias
  *   Target relation name (`alias:`), or None for the model name
  * @param engine
  *   Profile connector to build the model on (`engine:`), or None for the default engine
  * @param stageConfig
  *   Flow stage keys (retries, timeout, ...) passed through to the model's build stage
  */
case class ModelBuildSpec(
    materialization: Materialization,
    schema: Option[String],
    alias: Option[String],
    engine: Option[String],
    stageConfig: List[ConfigItem]
):
  def isMaterialized: Boolean = materialization != Materialization.Ephemeral

  /** The relation name the model is persisted to */
  def targetName(modelName: String): String =
    val name = alias.getOrElse(modelName)
    schema.map(s => s"${s}.${name}").getOrElse(name)

object ModelBuildSpec:
  private val materializationKeys = Set("materialized", "schema", "alias", "engine")

  /**
    * Flow stage keys a model passes through to its build stage. Activity-only keys (concurrency,
    * retry_uncertain, ...) are excluded, as a model build is a single SQL statement
    */
  private val stageKeys = Set("retries", "timeout", "retry_delay", "backoff", "max_retry_delay")

  val allowedKeys: Set[String] = materializationKeys ++ stageKeys

  private def invalid(msg: String, m: ModelDef, span: Span)(using ctx: Context) = StatusCode
    .INVALID_MODEL_CONFIG
    .newException(s"model ${m.name.name}: ${msg}", ctx.sourceLocationAt(span))

  /**
    * Validate the `with { }` config of a model. Unknown keys, unknown materializations, and
    * materialized models with parameters are compile errors
    */
  def of(m: ModelDef)(using ctx: Context): ModelBuildSpec =
    def nameValue(item: ConfigItem): String =
      item.value match
        case s: StringLiteral =>
          s.unquotedValue
        case n: NameExpr =>
          n.fullName
        case other =>
          throw invalid(s"${item.key.unquotedValue} must be a name or a string", m, other.span)

    var materialization: Materialization = Materialization.Ephemeral
    var schema: Option[String]           = None
    var alias: Option[String]            = None
    var engine: Option[String]           = None
    val stageConfig                      = List.newBuilder[ConfigItem]
    m.config
      .foreach { item =>
        item.key.unquotedValue match
          case "materialized" =>
            val name = nameValue(item)
            materialization = Materialization
              .fromName(name)
              .getOrElse(
                throw invalid(
                  s"unknown materialization '${name}'. Use one of: ${Materialization
                      .values
                      .map(_.configName)
                      .mkString(", ")}",
                  m,
                  item.span
                )
              )
          case "schema" =>
            schema = Some(nameValue(item))
          case "alias" =>
            alias = Some(nameValue(item))
          case "engine" =>
            engine = Some(nameValue(item))
          case k if stageKeys.contains(k) =>
            stageConfig += item
          case k =>
            throw invalid(
              s"unknown config key '${k}'. Allowed keys: ${allowedKeys
                  .toList
                  .sorted
                  .mkString(", ")}",
              m,
              item.span
            )
      }
    val spec = ModelBuildSpec(materialization, schema, alias, engine, stageConfig.result())
    if spec.isMaterialized && m.params.nonEmpty then
      throw invalid(
        s"a model with parameters cannot be materialized as ${materialization.configName}",
        m,
        m.span
      )
    if !spec.isMaterialized && (schema.nonEmpty || alias.nonEmpty || engine.nonEmpty) then
      throw invalid(
        "schema, alias, and engine apply only to materialized models. Add materialized: 'table' or 'view'",
        m,
        m.span
      )
    spec

  end of

end ModelBuildSpec

/**
  * A model together with its validated build config
  *
  * @param targetName
  *   The relation the model is persisted to, after `table ... in <catalog>.<schema>` bindings, so
  *   writes, downstream reads, and build checks all use the same relation
  */
case class BuildModel(model: ModelDef, spec: ModelBuildSpec, targetName: String):
  def name: String = model.name.name

/**
  * The dependency graph of the materialized models in a project. An edge points from a model to the
  * materialized models it reads, directly or through ephemeral models
  *
  * @param models
  *   Materialized models by name, in definition order
  * @param upstreams
  *   Materialized upstream models of each materialized model
  * @param ephemeral
  *   Ephemeral (inlined) models by name, traversed when reading through them
  */
case class ModelGraph(
    models: List[BuildModel],
    upstreams: Map[String, List[String]],
    ephemeral: Map[String, ModelDef] = Map.empty
):
  private val byName: Map[String, BuildModel] = models.map(m => m.name -> m).toMap

  def get(name: String): Option[BuildModel] = byName.get(name)

  lazy val downstreams: Map[String, List[String]] =
    models
      .map(m => m.name -> models.filter(d => upstreams(d.name).contains(m.name)).map(_.name))
      .toMap

  private def closure(start: String, next: String => List[String]): Set[String] =
    var visited = Set.empty[String]
    var queue   = List(start)
    while queue.nonEmpty do
      val n = queue.head
      queue = queue.tail
      if !visited.contains(n) then
        visited += n
        queue = next(n) ++ queue
    visited

  /**
    * Resolve dbt-style selectors: `m` selects the model, `m+` adds its downstream models, `+m` adds
    * its upstream models, and `+m+` adds both. No selector selects every model
    */
  def select(selectors: List[String]): List[BuildModel] =
    if selectors.isEmpty then
      models
    else
      val selected =
        selectors
          .flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
          .foldLeft(Set.empty[String]) { (acc, sel) =>
            val withUp   = sel.startsWith("+")
            val withDown = sel.endsWith("+") && sel.length > 1
            val name     = sel.stripPrefix("+").stripSuffix("+")
            if !byName.contains(name) then
              throw StatusCode
                .MODEL_NOT_FOUND
                .newException(
                  s"Model '${name}' is not a materialized model. Available models: ${models
                      .map(_.name)
                      .mkString(", ")}"
                )
            var s = Set(name)
            if withUp then
              s ++= closure(name, upstreams)
            if withDown then
              s ++= closure(name, downstreams)
            acc ++ s
          }
      models.filter(m => selected.contains(m.name))

  /** The models in build order: every model follows the models it reads */
  def buildOrder(selected: List[BuildModel]): List[BuildModel] =
    val names   = selected.map(_.name).toSet
    var done    = Set.empty[String]
    val ordered = List.newBuilder[BuildModel]
    var rest    = selected
    while rest.nonEmpty do
      val (ready, blocked) = rest.partition(m =>
        upstreams(m.name).filter(names.contains).forall(done.contains)
      )
      if ready.isEmpty then
        throw StatusCode
          .RECURSIVE_MODEL_REFERENCE
          .newException(s"Cyclic model dependency among: ${blocked.map(_.name).mkString(", ")}")
      ordered ++= ready
      done ++= ready.map(_.name)
      rest = blocked
    ordered.result()

end ModelGraph

/**
  * Plans `wvlet build`: collects the materialized models of a project, derives their dependency
  * graph from model references, and turns a selection into a flow with one stage per model, so
  * builds run on the flow executor (parallelism, retries, and recorded runs)
  */
object ModelBuildPlanner:
  /** Flow name build runs are recorded under */
  val BuildFlowName = "build"

  /**
    * Build the dependency graph of the given model definitions (typically all models of a project).
    * Ephemeral models are traversed, so a model reading an ephemeral model that reads a
    * materialized model depends on the materialized one
    */
  def graph(allModels: List[ModelDef])(using ctx: Context): ModelGraph =
    val defs         = allModels.map(m => m.name.name -> m).toMap
    val withSpec     = allModels.map(m => m -> ModelBuildSpec.of(m))
    val materialized = withSpec.collect {
      case (m, spec) if spec.isMaterialized =>
        BuildModel(m, spec, boundTarget(m, spec))
    }
    materialized
      .groupBy(_.name)
      .collect {
        case (name, ms) if ms.size > 1 =>
          throw StatusCode
            .INVALID_MODEL_CONFIG
            .newException(s"Materialized model ${name} is defined more than once")
      }
    val materializedNames = materialized.map(_.name).toSet

    def referencedModels(q: LogicalPlan): List[String] =
      val refs = List.newBuilder[String]
      q.traverse { case s: ModelScan =>
        refs += s.name.name
      }
      refs.result().distinct

    def upstreamsOf(m: ModelDef): List[String] =
      var visited = Set(m.name.name)
      val found   = List.newBuilder[String]
      var queue   = referencedModels(m.child)
      while queue.nonEmpty do
        val n = queue.head
        queue = queue.tail
        if !visited.contains(n) then
          visited += n
          if materializedNames.contains(n) then
            found += n
          else
            defs.get(n).foreach(d => queue = queue ++ referencedModels(d.child))
      found.result()

    ModelGraph(
      materialized,
      materialized.map(m => m.name -> upstreamsOf(m.model)).toMap,
      defs.filterNot((name, _) => materializedNames.contains(name))
    )

  end graph

  /**
    * Rewrite a model body so references to materialized models read their persisted relations. The
    * scan keeps the model's static type: the persisted relation is never looked up in the live
    * catalog. Ephemeral models without parameters are inlined here (as SQL generation would do), so
    * materialized models they read are also replaced
    */
  def readPersisted(q: Relation, graph: ModelGraph): Relation =
    def loop(r: Relation, inlining: Set[String]): Relation = r
      .transformUp {
        case s: ModelScan if graph.get(s.name.name).nonEmpty =>
          val target = graph.get(s.name.name).get
          val tpe    = target.model.relationType
          TableScan(TableName.parse(target.targetName), tpe, tpe.fields, s.span)
        case s: ModelScan
            if s.modelArgs.isEmpty && graph.ephemeral.contains(s.name.name) &&
              !inlining.contains(s.name.name) =>
          val md = graph.ephemeral(s.name.name)
          loop(md.child.body, inlining + s.name.name)
      }
      .asInstanceOf[Relation]
    loop(q, Set.empty)

  private def targetExpr(name: String): QualifiedName =
    name.split("\\.").toList.map(n => UnquotedIdentifier(n, NoSpan)) match
      case head :: tail =>
        tail.foldLeft(head: QualifiedName)((q, n) => DotRef(q, n, DataType.UnknownType, NoSpan))
      case Nil =>
        throw StatusCode.INVALID_ARGUMENT.newException("empty target name")

  /**
    * The relation a model is persisted to. A `table` declaration bound to a location already pins
    * where writes land, so the model follows it, and a conflicting schema/alias is an error
    */
  private def boundTarget(m: ModelDef, spec: ModelBuildSpec)(using ctx: Context): String =
    if spec.schema.nonEmpty || spec.alias.nonEmpty then
      TableBindings
        .declaredBindingOf(m.name.name)
        .foreach { case (c, s) =>
          throw StatusCode
            .INVALID_MODEL_CONFIG
            .newException(
              s"model ${m
                  .name
                  .name}: schema/alias conflict with the table declaration bound to ${c}.${s}. Remove one of them",
              ctx.sourceLocationAt(m.span)
            )
        }
    val name = spec.targetName(m.name.name)
    TableBindings.declarationFor(name).flatMap(_.boundName).map(_.fullName).getOrElse(name)

  /** The statement persisting a model: `save to <target>` or `save as view <target>` */
  def saveStatement(m: BuildModel, graph: ModelGraph): Save =
    val body   = readPersisted(m.model.child, graph)
    val target = targetExpr(m.targetName)
    m.spec.materialization match
      case Materialization.View =>
        CreateView(target, replace = true, body, m.model.span)
      case _ =>
        SaveTo(body, target, Nil, m.model.span)

  /**
    * Plan a build of the selected models as a flow: one stage per model, each depending on the
    * selected models it reads. Unselected upstream models are read from their persisted relations
    */
  def toFlow(graph: ModelGraph, selected: List[BuildModel])(using ctx: Context): FlowDef =
    val names  = selected.map(_.name).toSet
    val stages = graph
      .buildOrder(selected)
      .map { m =>
        StageDef(
          name = Name.termName(m.name),
          inputRefs = Nil,
          dependsOn = graph
            .upstreams(m.name)
            .filter(names.contains)
            .map(n => UnquotedIdentifier(n, NoSpan)),
          trigger = None,
          config = m.spec.stageConfig,
          body = Some(saveStatement(m, graph)),
          span = m.model.span,
          engine = m.spec.engine.map(e => UnquotedIdentifier(e, NoSpan))
        )
      }
    FlowDef(Name.termName(BuildFlowName), None, Nil, Nil, stages, NoSpan)

end ModelBuildPlanner

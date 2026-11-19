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

import wvlet.lang.api.StatusCode

/**
  * A parsed dbt-style selector: `+name` adds upstream models, `name+` adds downstream models
  */
case class ModelSelector(name: String, withUpstream: Boolean, withDownstream: Boolean)

/**
  * The graph algorithms of `wvlet build`, over model names: edges through ephemeral models,
  * selector closures, build order, and cycle detection.
  *
  * Modelled and proved in Lean under formal/WvletFormal/ModelBuild (Graph.lean, Order.lean), and
  * replayed by `ModelBuildConformanceTest`. When changing these functions, update the Lean model
  * first and regenerate the fixture with `cd formal && lake exe model-build-traces`.
  */
object ModelOrder:

  /**
    * The nodes reachable from `start` (including it), by a depth-first search. Terminates on any
    * finite graph: each step visits a new node or pops one already visited
    */
  def closure(start: String, next: String => List[String]): Set[String] =
    var visited = Set.empty[String]
    var queue   = List(start)
    while queue.nonEmpty do
      val n = queue.head
      queue = queue.tail
      if !visited.contains(n) then
        visited += n
        queue = next(n) ++ queue
    visited

  /** The models reading each model directly, in definition order */
  def downstreams(
      models: List[String],
      upstreams: Map[String, List[String]]
  ): Map[String, List[String]] =
    models.map(m => m -> models.filter(d => upstreams(d).contains(m))).toMap

  /**
    * The materialized models a model reads, through ephemeral models, in breadth-first order. A
    * model reaching its own name gets a self-edge, so self-references are reported as cycles
    *
    * @param direct
    *   The models the model references directly
    * @param isMaterialized
    *   Whether a name is a materialized model
    * @param refs
    *   The direct references of a defined model, or None for an unknown name
    */
  def upstreamsOf(
      direct: List[String],
      isMaterialized: String => Boolean,
      refs: String => Option[List[String]]
  ): List[String] =
    var visited = Set.empty[String]
    val found   = List.newBuilder[String]
    var queue   = direct
    while queue.nonEmpty do
      val n = queue.head
      queue = queue.tail
      if !visited.contains(n) then
        visited += n
        if isMaterialized(n) then
          found += n
        else
          refs(n).foreach(r => queue = queue ++ r)
    found.result()

  /**
    * Parse selector arguments: each argument is split on commas, tokens are trimmed, and empty
    * tokens are dropped. One leading `+` selects upstream models, one trailing `+` downstream ones
    */
  def parseSelectors(selectors: List[String]): List[ModelSelector] = selectors
    .flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
    .map { sel =>
      ModelSelector(
        name = sel.stripPrefix("+").stripSuffix("+"),
        withUpstream = sel.startsWith("+"),
        withDownstream = sel.endsWith("+") && sel.length > 1
      )
    }

  /**
    * Resolve dbt-style selectors: `m` selects the model, `m+` adds its downstream models, `+m` adds
    * its upstream models, and `+m+` adds both. No selector selects every model. Returns models in
    * definition order
    */
  def select(
      models: List[String],
      upstreams: Map[String, List[String]],
      selectors: List[String]
  ): List[String] =
    if selectors.isEmpty then
      models
    else
      val parsed = parseSelectors(selectors)
      if parsed.isEmpty then
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"No model selected: selectors ${selectors
                .map(s => s"'${s}'")
                .mkString(", ")} name no model"
          )
      val names = models.toSet
      parsed
        .find(s => !names.contains(s.name))
        .foreach { s =>
          throw StatusCode
            .MODEL_NOT_FOUND
            .newException(
              s"Model '${s.name}' is not a materialized model. Available models: ${models.mkString(
                  ", "
                )}"
            )
        }
      lazy val downs = downstreams(models, upstreams)
      val selected   =
        parsed.foldLeft(Set.empty[String]) { (acc, sel) =>
          var s = Set(sel.name)
          if sel.withUpstream then
            s ++= closure(sel.name, upstreams)
          if sel.withDownstream then
            s ++= closure(sel.name, downs)
          acc ++ s
        }
      models.filter(selected.contains)

  end select

  /**
    * Order models for building in rounds: each round is every remaining model whose upstream models
    * in `selected` are built, in input order
    *
    * @return
    *   Right(build order), or Left(a dependency cycle among the selected models, first model
    *   repeated last)
    */
  def tryBuildOrder(
      selected: List[String],
      upstreams: Map[String, List[String]]
  ): Either[List[String], List[String]] =
    val names   = selected.toSet
    var done    = Set.empty[String]
    val ordered = List.newBuilder[String]
    var rest    = selected
    var cycle   = Option.empty[List[String]]
    while rest.nonEmpty && cycle.isEmpty do
      val (ready, blocked) = rest.partition(m =>
        upstreams(m).filter(names.contains).forall(done.contains)
      )
      if ready.isEmpty then
        cycle = Some(cycleIn(blocked, upstreams))
      else
        ordered ++= ready
        done ++= ready
        rest = blocked
    cycle.toLeft(ordered.result())

  /** The models in build order: every model follows the selected models it reads */
  def buildOrder(selected: List[String], upstreams: Map[String, List[String]]): List[String] =
    tryBuildOrder(selected, upstreams) match
      case Right(order) =>
        order
      case Left(cycle) =>
        throw StatusCode.RECURSIVE_MODEL_REFERENCE.newException(cycleMessage(cycle))

  /** A dependency cycle among the models, if any, with its first model repeated last */
  def findCycle(models: List[String], upstreams: Map[String, List[String]]): Option[List[String]] =
    tryBuildOrder(models, upstreams).left.toOption

  def cycleMessage(cycle: List[String]): String =
    s"Cyclic model dependency: ${cycle.mkString(" -> ")}"

  /**
    * A cycle through models that no round could build. Every such model reads another one of them,
    * so following the first such upstream model from the first model always repeats a model
    */
  private def cycleIn(blocked: List[String], upstreams: Map[String, List[String]]): List[String] =
    val inBlocked               = blocked.toSet
    def next(n: String): String = upstreams(n)
      .find(inBlocked.contains)
      .getOrElse(throw StatusCode.UNEXPECTED_STATE.newException(s"${n} reads no blocked model"))
    var path = Vector(blocked.head)
    var cur  = next(blocked.head)
    while !path.contains(cur) do
      path :+= cur
      cur = next(cur)
    (path.dropWhile(_ != cur) :+ cur).toList

end ModelOrder

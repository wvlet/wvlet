/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.external

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.ext.ExternalFunction
import wvlet.lang.ext.FunctionProvider
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.log.LogSupport

import java.io.File
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import scala.jdk.CollectionConverters.*

/** Where the implementation of a `def ... = native` function was found */
enum ExternalFunctionImpl:
  /** A function provided by a JVM [[FunctionProvider]] (application classpath or a plugin jar) */
  case Jvm(function: ExternalFunction)

  /** A function exported by a TypeScript/JavaScript module, run through the Node host */
  case NodeModule(name: String, module: File)

/**
  * Resolves `def ... = native` functions by name at execution time. Implementations come from:
  *   - JVM [[FunctionProvider]]s discovered with `ServiceLoader`, on the application classpath and
  *     in `*.jar` files of the plugin directories
  *   - functions exported by `*.ts` / `*.mts` / `*.js` / `*.mjs` modules in the plugin directories
  *
  * A name provided more than once is an error rather than first-wins, so a query never silently
  * changes behavior when a plugin is added.
  *
  * @param pluginDirs
  *   directories scanned (non-recursively) for plugin jars and modules
  * @param extraFunctions
  *   functions registered programmatically (embedding and tests)
  */
class ExternalFunctionRegistry(
    val pluginDirs: Seq[File],
    extraFunctions: Seq[ExternalFunction] = Nil,
    nodeCommand: String = sys.env.getOrElse("WVLET_NODE", "node")
) extends LogSupport:
  import ExternalFunctionRegistry.*

  private def pluginFiles(extensions: Set[String]): Seq[File] = pluginDirs
    .filter(_.isDirectory)
    .flatMap(d => Option(d.listFiles()).toSeq.flatten)
    .filter(f => f.isFile && extensions.exists(ext => f.getName.endsWith(ext)))
    .sortBy(_.getPath)

  private lazy val jvmFunctions: Map[String, ExternalFunction] =
    val parent =
      Thread.currentThread().getContextClassLoader match
        case null =>
          getClass.getClassLoader
        case cl =>
          cl
    val jars   = pluginFiles(Set(".jar"))
    val loader =
      if jars.isEmpty then
        parent
      else
        URLClassLoader(jars.map(_.toURI.toURL).toArray, parent)
    val provided =
      java
        .util
        .ServiceLoader
        .load(classOf[FunctionProvider], loader)
        .iterator()
        .asScala
        .flatMap(_.functions)
        .toList
    uniqueByName((extraFunctions ++ provided).map(f => f.name -> f), "JVM function providers")

  lazy val moduleFiles: Seq[File] = pluginFiles(moduleExtensions)

  // The exports of every plugin module, listed once by the Node host
  private lazy val nodeExports: Map[String, File] =
    if moduleFiles.isEmpty then
      Map.empty
    else
      val listing =
        ExternalProcess.run(
          label = "plugin module listing",
          command = nodeCommandLine("list", moduleFiles),
          env = nodeEnv,
          workDir = File("."),
          input = Iterator.empty,
          onStderr = line => debug(line),
          register = _ => (),
          deregister = () => ()
        )(out => Files.readString(out))
      JSON.parse(listing) match
        case o: JSONObject =>
          o.v
            .collect { case (name, JSONString(path)) =>
              name -> File(path)
            }
            .toMap
        case other =>
          throw StatusCode
            .EXTERNAL_FUNCTION_FAILED
            .newException(s"Unexpected plugin module listing: ${other.toJSON}")

  /** Find the implementation of the function with the given name */
  def find(name: String): Option[ExternalFunctionImpl] =
    val jvm  = jvmFunctions.get(name).map(ExternalFunctionImpl.Jvm(_))
    val node = nodeExports.get(name).map(ExternalFunctionImpl.NodeModule(name, _))
    (jvm, node) match
      case (Some(_), Some(ExternalFunctionImpl.NodeModule(_, module))) =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(
            s"Function '${name}' is provided by both a JVM plugin and the module ${module.getPath}"
          )
      case _ =>
        jvm.orElse(node)

  /** Environment of the Node host: keeps the type-stripping warning out of the function logs */
  val nodeEnv: Map[String, String] = Map("NODE_NO_WARNINGS" -> "1")

  /** True when no plugin could provide any function, so lookups can be skipped entirely */
  def isEmpty: Boolean = jvmFunctions.isEmpty && moduleFiles.isEmpty

  /** The command line running the Node host in the given mode over the given modules */
  def nodeCommandLine(
      mode: String,
      modules: Seq[File],
      functionName: Option[String] = None
  ): Seq[String] =
    val stripTypes =
      if modules.exists(m => m.getName.endsWith(".ts") || m.getName.endsWith(".mts")) then
        Seq("--experimental-strip-types")
      else
        Nil
    Seq(nodeCommand) ++ stripTypes ++ Seq(nodeHostScript.getPath, mode) ++ functionName.toSeq ++
      modules.map(_.getAbsolutePath)

end ExternalFunctionRegistry

object ExternalFunctionRegistry:
  private val moduleExtensions = Set(".ts", ".mts", ".js", ".mjs")

  /** Name of the conventional plugin directory under the working folder */
  val defaultPluginDirName = "plugins"

  /** Environment variable listing additional plugin directories (path-separator delimited) */
  val pluginPathEnv = "WVLET_PLUGIN_PATH"

  /**
    * The registry for a working folder: `<workdir>/plugins` plus the directories listed in
    * `WVLET_PLUGIN_PATH`
    */
  def forWorkEnv(workEnv: WorkEnv): ExternalFunctionRegistry =
    val fromEnv = sys
      .env
      .get(pluginPathEnv)
      .toSeq
      .flatMap(_.split(File.pathSeparator))
      .filter(_.nonEmpty)
      .map(File(_))
    ExternalFunctionRegistry(File(workEnv.path, defaultPluginDirName) +: fromEnv)

  private def uniqueByName[A](entries: Seq[(String, A)], source: String): Map[String, A] = entries
    .groupBy(_._1)
    .map { (name, defs) =>
      if defs.size > 1 then
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Function '${name}' is provided ${defs.size} times by ${source}")
      name -> defs.head._2
    }

  // The Node host ships as a classpath resource; node needs it as a file
  private lazy val nodeHostScript: File =
    val resource = "/wvlet/lang/runner/external/wvlet-udf-host.mjs"
    val in       = getClass.getResourceAsStream(resource)
    if in == null then
      throw StatusCode.INTERNAL_ERROR.newException(s"Missing resource: ${resource}")
    try
      val file = Files.createTempFile("wvlet-udf-host-", ".mjs")
      Files.copy(in, file, StandardCopyOption.REPLACE_EXISTING)
      file.toFile.deleteOnExit()
      file.toFile
    finally
      in.close()

end ExternalFunctionRegistry

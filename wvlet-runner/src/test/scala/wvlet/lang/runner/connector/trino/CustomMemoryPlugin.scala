package wvlet.lang.runner.connector.trino

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.common.collect.ImmutableList
import com.google.inject.{Inject, Injector, Scopes}
import io.airlift.bootstrap.{Bootstrap, LifeCycleManager}
import io.airlift.json.JsonModule
import io.airlift.slice.{Slice, Slices}
import io.trino.plugin.memory.*
import io.trino.spi.Plugin
import io.trino.spi.`type`.VarcharType
import io.trino.spi.block.Block
import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch
import io.trino.spi.connector.*
import io.trino.spi.function.FunctionProvider
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable
import io.trino.spi.function.table.*
import wvlet.log.LogSupport

import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.{lang, util}
import scala.jdk.CollectionConverters.*

class CustomMemoryPlugin extends Plugin:
  override def getConnectorFactories: lang.Iterable[ConnectorFactory] = ImmutableList
    .of(CustomMemoryConnectorFactory())

class CustomMemoryModule extends com.google.inject.Module:
  override def configure(binder: com.google.inject.Binder): Unit =
    binder.bind(classOf[CustomMemoryConnector]).in(Scopes.SINGLETON)
    binder.bind(classOf[CustomMemorySplitManager]).in(Scopes.SINGLETON)

class CustomMemoryConnectorFactory extends MemoryConnectorFactory:

  override def getName: String = "wvlet"

  override def create(
      catalogName: String,
      requiredConfig: java.util.Map[String, String],
      context: ConnectorContext
  ): CustomMemoryConnector =
    // A plugin is not required to use Guice; it is just very convenient
    val app =
      new Bootstrap(
        new JsonModule,
        new MemoryModule(context.getTypeManager, context.getNodeManager),
        CustomMemoryModule()
      )

    val injector =
      app.doNotInitializeLogging.setRequiredConfigurationProperties(requiredConfig).initialize

    injector.getInstance(classOf[CustomMemoryConnector])

class CustomMemoryConnector @Inject (
    lifeCycleManager: LifeCycleManager,
    metadata: MemoryMetadata,
    splitManager: CustomMemorySplitManager,
    pageSourceProvider: MemoryPageSourceProvider,
    pageSinkProvider: MemoryPageSinkProvider
) extends MemoryConnector(
      lifeCycleManager,
      metadata,
      splitManager.memorySplitManager,
      pageSourceProvider,
      pageSinkProvider
    ):

  override def getTableFunctions: util.Set[ConnectorTableFunction] = Set(HelloTableFunction).asJava

  override def getFunctionProvider: Optional[FunctionProvider] = Optional
    .of(CustomMemoryFunctionProvider)

  override def getSplitManager: ConnectorSplitManager = splitManager

object CustomMemoryFunctionProvider extends FunctionProvider:
  override def getTableFunctionProcessorProvider(
      functionHandle: ConnectorTableFunctionHandle
  ): TableFunctionProcessorProvider =
    functionHandle match
      case h: HelloTableFunction.HelloTableFunctionHandle =>
        HelloTableFunction.TableFunctionProcessor()
      case _ =>
        super.getTableFunctionProcessorProvider(functionHandle)

class CustomMemorySplitManager @Inject (config: MemoryConfig, metadata: MemoryMetadata)
    extends ConnectorSplitManager
    with LogSupport:
  val memorySplitManager = MemorySplitManager(config, metadata)

  override def getSplits(
      transaction: ConnectorTransactionHandle,
      session: ConnectorSession,
      table: ConnectorTableHandle,
      dynamicFilter: DynamicFilter,
      constraint: Constraint
  ): ConnectorSplitSource = memorySplitManager
    .getSplits(transaction, session, table, dynamicFilter, constraint)

  override def getSplits(
      transaction: ConnectorTransactionHandle,
      session: ConnectorSession,
      function: ConnectorTableFunctionHandle
  ): ConnectorSplitSource =
    function match
      case h: HelloTableFunction.HelloTableFunctionHandle =>
        warn(s"here: ${function}")
        new HelloTableFunction.SplitSource()
      case other =>
        super.getSplits(transaction, session, function)

object HelloTableFunction extends ConnectorTableFunction:
  override def getName: String = "hello"

  override def getSchema: String = "wvlet"

  override def getArguments: util.List[ArgumentSpecification] =
    List(
      ScalarArgumentSpecification
        .builder()
        .name("message")
        .`type`(io.trino.spi.`type`.VarcharType.VARCHAR)
        .build()
    ).asJava

  override def getReturnTypeSpecification: ReturnTypeSpecification =
    ReturnTypeSpecification.GenericTable.GENERIC_TABLE

  override def analyze(
      session: ConnectorSession,
      transaction: ConnectorTransactionHandle,
      arguments: util.Map[String, Argument],
      accessControl: ConnectorAccessControl
  ): TableFunctionAnalysis =

    val fields =
      List(Descriptor.Field("message", Optional.of(io.trino.spi.`type`.VarcharType.VARCHAR))).asJava
    val returnedType = Descriptor(fields)

    val message: String =
      arguments.get("message") match
        case a: io.trino.spi.function.table.ScalarArgument =>
          a.getValue match
            case s: Slice =>
              s.toStringUtf8
            case _ =>
              "N/A"
        case _ =>
          "N/A"

    TableFunctionAnalysis
      .builder()
      .returnedType(returnedType)
      .handle(HelloTableFunctionHandle(message))
      .build()

  case class HelloTableFunctionHandle @JsonCreator() (
      @JsonProperty("msg")
      msg: String
  ) extends ConnectorTableFunctionHandle:
    @JsonProperty
    def getMsg: String = msg

  class SplitSource extends ConnectorSplitSource:
    private val isRead = AtomicBoolean(false)

    override def getNextBatch(maxSize: Int): CompletableFuture[ConnectorSplitBatch] =
      if isRead.compareAndSet(false, true) then
        java
          .util
          .concurrent
          .CompletableFuture
          .completedFuture(ConnectorSplitBatch(List(Split(0)).asJava, true))
      else
        throw new UnsupportedOperationException("No more splits")

    override def close(): Unit = {
      // Do nothing
    }

    override def isFinished: Boolean = isRead.get()

  class TableFunctionProcessor extends TableFunctionProcessorProvider:
    override def getSplitProcessor(
        session: ConnectorSession,
        handle: ConnectorTableFunctionHandle,
        split: ConnectorSplit
    ): TableFunctionSplitProcessor = SplitProcessor()

  class Split @JsonCreator (
      @JsonProperty("id")
      id: Int
  ) extends ConnectorSplit:
    @JsonProperty
    def getId: Int = id

  class SplitProcessor extends io.trino.spi.function.table.TableFunctionSplitProcessor:
    private var count = 0
    override def process(): TableFunctionProcessorState =
      if count == 0 then
        count += 1
        val result =
          new io.trino.spi.Page(
            VarcharType
              .VARCHAR
              .createBlockBuilder(null, 1)
              .writeEntry(Slices.utf8Slice("hello"))
              .build()
          )
        TableFunctionProcessorState.Processed.produced(result)
      else
        TableFunctionProcessorState.Finished.FINISHED

end HelloTableFunction

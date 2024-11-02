package wvlet.lang.runner.connector.trino

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.google.common.collect.ImmutableList
import com.google.inject.{Inject, Injector, Scopes}
import io.airlift.bootstrap.{Bootstrap, LifeCycleManager}
import io.airlift.json.JsonModule
import io.airlift.slice.{Slice, Slices}
import io.trino.plugin.memory.*
import io.trino.spi.{Page, Plugin}
import io.trino.spi.`type`.{BooleanType, DoubleType, IntegerType, VarcharType}
import io.trino.spi.block.{Block, VariableWidthBlockBuilder}
import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch
import io.trino.spi.connector.*
import io.trino.spi.function.FunctionProvider
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable
import io.trino.spi.function.table.*
import wvlet.airframe.codec.JDBCCodec.ResultSetCodec
import wvlet.airframe.codec.MessageCodec
import wvlet.airframe.msgpack.spi.MessagePack
import wvlet.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.lang.runner.connector.trino.DuckDBSQLFunction.{DuckDBFunctionHandle, DuckDBQuerySplit}
import wvlet.log.LogSupport

import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.{lang, util}
import scala.collection.immutable.ListMap
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

  override def getTableFunctions: util.Set[ConnectorTableFunction] =
    Set(HelloTableFunction, DuckDBSQLFunction).asJava

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
      case d: DuckDBFunctionHandle =>
        DuckDBSQLFunction.QuerySplitProcessor()
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
        FixedSplitSource(HelloTableFunction.Split(h.msg))
      case d: DuckDBFunctionHandle =>
        FixedSplitSource(DuckDBQuerySplit(d.sql))
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

  class TableFunctionProcessor extends TableFunctionProcessorProvider:
    override def getSplitProcessor(
        session: ConnectorSession,
        handle: ConnectorTableFunctionHandle,
        split: ConnectorSplit
    ): TableFunctionSplitProcessor =
      split match
        case s: Split =>
          SplitProcessor(s.getMsg)
        case _ =>
          SplitProcessor("N/A")

  case class Split @JsonCreator (
      @JsonProperty("msg")
      msg: String
  ) extends ConnectorSplit:
    @JsonProperty
    def getMsg: String = msg

  class SplitProcessor(msg: String) extends io.trino.spi.function.table.TableFunctionSplitProcessor:
    private var count = 0
    override def process(): TableFunctionProcessorState =
      if count == 0 then
        count += 1
        val result =
          new io.trino.spi.Page(
            VarcharType
              .VARCHAR
              .createBlockBuilder(null, 1)
              .writeEntry(Slices.utf8Slice(s"Hello ${msg}!"))
              .build()
          )
        TableFunctionProcessorState.Processed.produced(result)
      else
        TableFunctionProcessorState.Finished.FINISHED

end HelloTableFunction

/**
  * Run a DuckDB SQL inside Trino
  */
object DuckDBSQLFunction extends ConnectorTableFunction with LogSupport:

  private def toTrinoType(typeName: String): io.trino.spi.`type`.Type =
    typeName match
      case "INTEGER" =>
        IntegerType.INTEGER
      case "DOUBLE" =>
        DoubleType.DOUBLE
      case "VARCHAR" =>
        VarcharType.VARCHAR
      case "BOOLEAN" =>
        BooleanType.BOOLEAN
      case other =>
        warn(s"Unknown type: ${other}")
        VarcharType.VARCHAR

  // TODO: Pass this from connector dependency
  private val duckdb: DuckDBConnector = DuckDBConnector()

  override def getName: String = "sql"

  override def getSchema: String = "duckdb"

  override def getReturnTypeSpecification: ReturnTypeSpecification =
    ReturnTypeSpecification.GenericTable.GENERIC_TABLE

  override def getArguments: util.List[ArgumentSpecification] =
    List(
      ScalarArgumentSpecification
        .builder()
        .name("sql")
        .`type`(io.trino.spi.`type`.VarcharType.VARCHAR)
        .build()
    ).asJava

  override def analyze(
      session: ConnectorSession,
      transaction: ConnectorTransactionHandle,
      arguments: util.Map[String, Argument],
      accessControl: ConnectorAccessControl
  ): TableFunctionAnalysis =

    val sql: String =
      arguments.get("sql") match
        case a: io.trino.spi.function.table.ScalarArgument =>
          a.getValue match
            case s: Slice =>
              s.toStringUtf8
            case _ =>
              "N/A"
        case _ =>
          "N/A"

    val returnedType =
      duckdb.runQuery(s"describe ${sql}") { rs =>
        val fields = List.newBuilder[Descriptor.Field]
        while rs.next() do
          val columName = rs.getString("column_name")
          val columType = toTrinoType(rs.getString("column_type"))
          fields += Descriptor.Field(columName, Optional.of(columType))
        Descriptor(fields.result().asJava)
      }

    TableFunctionAnalysis
      .builder()
      .returnedType(returnedType)
      .handle(DuckDBFunctionHandle(sql))
      .build()
  end analyze

  case class DuckDBFunctionHandle @JsonCreator() (
      @JsonProperty("sql")
      sql: String
  ) extends ConnectorTableFunctionHandle:
    @JsonProperty
    def getSql: String = sql

  case class DuckDBQuerySplit @JsonCreator (
      @JsonProperty("sql")
      sql: String
  ) extends ConnectorSplit:
    @JsonProperty
    def getSql: String = sql

  class QuerySplitProcessor extends TableFunctionProcessorProvider:
    override def getSplitProcessor(
        session: ConnectorSession,
        handle: ConnectorTableFunctionHandle,
        split: ConnectorSplit
    ): TableFunctionSplitProcessor =
      split match
        case s: DuckDBQuerySplit =>
          DuckDBSplitProcessor(s.sql)
        case _ =>
          ???

  class DuckDBSplitProcessor(sql: String) extends TableFunctionSplitProcessor with LogSupport:
    private val isProcessed = AtomicBoolean(false)

    private def toPage(
        columnNames: IndexedSeq[String],
        types: IndexedSeq[io.trino.spi.`type`.Type],
        records: Seq[ListMap[String, Any]]
    ): Page =
      val blockBuilders = types.map(_.createBlockBuilder(null, records.size)).toIndexedSeq
      records.foreach { record =>
        record
          .zipWithIndex
          .foreach { case ((k, v), i) =>
            val blockBuilder = blockBuilders(i)
            v match
              case null =>
                blockBuilder.appendNull()
              case _ =>
                types(i) match
                  case IntegerType.INTEGER =>
                    v match
                      case l: Long =>
                        IntegerType.INTEGER.writeLong(blockBuilder, l)
                      case i: Int =>
                        IntegerType.INTEGER.writeInt(blockBuilder, i)
                  case DoubleType.DOUBLE =>
                    DoubleType.DOUBLE.writeDouble(blockBuilder, v.asInstanceOf[Double])
                  case VarcharType.VARCHAR =>
                    blockBuilder
                      .asInstanceOf[VariableWidthBlockBuilder]
                      .writeEntry(Slices.utf8Slice(v.toString))
                  case BooleanType.BOOLEAN =>
                    BooleanType.BOOLEAN.writeBoolean(blockBuilder, v.asInstanceOf[Boolean])
                  case other =>
                    blockBuilder
                      .asInstanceOf[VariableWidthBlockBuilder]
                      .writeEntry(Slices.utf8Slice(v.toString))
          }
      }
      val blocks = blockBuilders.map(_.build())
      new Page(blocks*)

    end toPage

    override def process(): TableFunctionProcessorState =
      if isProcessed.compareAndSet(false, true) then
        val page =
          duckdb.runQuery(sql) { rs =>
            val md          = rs.getMetaData
            val columnCount = md.getColumnCount
            val columnNames = (1 to columnCount).map(i => md.getColumnName(i))
            debug(s"column names: ${columnNames}")
            val types = (1 to columnCount).map(i => md.getColumnTypeName(i)).map(toTrinoType)
            debug(s"column types: ${types}")

            val json    = ResultSetCodec(rs).toJson
            val records = MessageCodec.of[Seq[ListMap[String, Any]]].fromJson(json)
            debug(s"=== ${records}")
            toPage(columnNames.toIndexedSeq, types.toIndexedSeq, records)
          }
        TableFunctionProcessorState.Processed.produced(page)
      else
        TableFunctionProcessorState.Finished.FINISHED

  end DuckDBSplitProcessor

end DuckDBSQLFunction

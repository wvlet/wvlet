package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.codegen.GenSQL
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Compiler, Context, Name}
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{NamedType, SchemaType, UnresolvedType}
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.runner.connector.DBContext
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBContext
import org.duckdb.DuckDBConnection
import wvlet.airframe.codec.{JDBCCodec, MessageCodec}
import wvlet.airframe.metrics.ElapsedTime
import wvlet.log.{LogLevel, LogSupport, Logger}

import java.sql.{DriverManager, SQLException}
import scala.collection.immutable.ListMap
import scala.util.{Try, Using}

object QueryExecutor extends LogSupport:
  def default: QueryExecutor = QueryExecutor(dbContext = DuckDBContext())

class QueryExecutor(dbContext: DBContext) extends LogSupport with AutoCloseable:

  override def close(): Unit = dbContext.close()

  def execute(sourceFolder: String, file: String): Unit =
    val result = Compiler(sourceFolders = List(sourceFolder), contextFolder = sourceFolder)
      .compileSingle(Some(file))
    result
      .inFile(file)
      .foreach: u =>
        execute(u, result.context)

  def execute(u: CompilationUnit, context: Context, limit: Int = 40): QueryResult =
    val result = execute(u.resolvedPlan, context, limit)
    result

  def execute(plan: LogicalPlan, context: Context, limit: Int): QueryResult =
    plan match
      case p: PackageDef =>
        val results = p
          .statements
          .map { stmt =>
            PlanResult(stmt, execute(stmt, context, limit))
          }
        QueryResultList(results)
      case q: Query =>
        val generatedSQL = GenSQL.generateSQL(q, context)
        debug(s"Executing SQL:\n${generatedSQL.sql}")
        try
          val result = dbContext.withConnection { conn =>
            Using.resource(conn.createStatement()) { stmt =>
              Using.resource(stmt.executeQuery(generatedSQL.sql)) { rs =>
                val metadata = rs.getMetaData
                val fields =
                  for i <- 1 to metadata.getColumnCount
                  yield NamedType(
                    Name.termName(metadata.getColumnName(i)),
                    Try(DataType.parse(metadata.getColumnTypeName(i))).getOrElse {
                      UnresolvedType(metadata.getColumnTypeName(i))
                    }
                  )
                val outputType = SchemaType(None, Name.NoTypeName, fields)

                val codec    = JDBCCodec(rs)
                val rowCodec = MessageCodec.of[ListMap[String, Any]]
                val it = codec.mapMsgPackMapRows { msgpack =>
                  rowCodec.fromMsgPack(msgpack)
                }
                var rowCount = 0
                val rows     = Seq.newBuilder[ListMap[String, Any]]
                while it.hasNext do
                  val row = it.next()
                  if rowCount < limit then
                    rows += row
                  rowCount += 1

                TableRows(outputType, rows.result(), rowCount)
              }
            }
          }

          result
        catch
          case e: SQLException =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"Failed to execute SQL: ${generatedSQL.sql}\n${e.getMessage}", e)
        end try
      case t: TableDef =>
        QueryResult.empty
      case t: TestRelation =>
        debug(s"Executing test: ${t}")
        QueryResult.empty
      case m: ModelDef =>
        QueryResult.empty
      case s: Subscribe =>
        debug(s"Executing subscribe: ${s}")
        QueryResult.empty
      case f: LanguageStatement =>
        QueryResult.empty
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported plan: ${other}")

end QueryExecutor

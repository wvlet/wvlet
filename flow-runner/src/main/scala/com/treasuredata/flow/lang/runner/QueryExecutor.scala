package com.treasuredata.flow.lang.runner

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.*
import com.treasuredata.flow.lang.compiler.codegen.GenSQL
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.DataType.{NamedType, SchemaType, UnresolvedType}
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.runner.connector.DBConnector
import com.treasuredata.flow.lang.runner.connector.duckdb.DuckDBConnector
import wvlet.airframe.codec.{JDBCCodec, MessageCodec}
import wvlet.airframe.control.Control
import wvlet.airframe.control.Control.withResource
import wvlet.log.{LogLevel, LogSupport}

import java.sql.SQLException
import scala.collection.immutable.ListMap
import scala.util.{Try, Using}

object QueryExecutor extends LogSupport:
  def default: QueryExecutor = QueryExecutor(dbConnector = DuckDBConnector())

class QueryExecutor(dbConnector: DBConnector) extends LogSupport with AutoCloseable:

  def getDBConnector: DBConnector = dbConnector

  override def close(): Unit = dbConnector.close()

  def executeSingleSpec(sourceFolder: String, file: String): QueryResult =
    val compiler = Compiler(
      CompilerOptions(sourceFolders = List(sourceFolder), workingFolder = sourceFolder)
    )
    compiler.compilationUnitsInSourcePaths.find(_.sourceFile.fileName == file) match
      case Some(unit) =>
        val result = compiler.compileSingleUnit(unit)
        executeSingle(unit, result.context)
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"File not found: ${file}")

  def executeSingle(u: CompilationUnit, context: Context, limit: Int = 40): QueryResult =
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
        trace(s"[plan]:\n${generatedSQL.plan.pp}")
        try
          val result = dbConnector.withConnection { conn =>
            withResource(conn.createStatement()) { stmt =>
              withResource(stmt.executeQuery(generatedSQL.sql)) { rs =>
                dbConnector.processWarning(stmt.getWarnings())

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
                trace(outputType)

                val codec    = JDBCCodec(rs)
                val rowCodec = MessageCodec.of[ListMap[String, Any]]
                var rowCount = 0
                val it = codec.mapMsgPackMapRows { msgpack =>
                  if rowCount < limit then
                    rowCodec.fromMsgPack(msgpack)
                  else
                    null
                }

                val rows = Seq.newBuilder[ListMap[String, Any]]
                while it.hasNext do
                  val row = it.next()
                  if row != null && rowCount < limit then
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
              .newException(s"${e.getMessage}\n[sql]\n${generatedSQL.sql}", e)
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

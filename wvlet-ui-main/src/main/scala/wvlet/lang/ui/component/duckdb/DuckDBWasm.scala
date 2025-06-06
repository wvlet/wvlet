package wvlet.lang.ui.component.duckdb

import wvlet.airframe.rx.Rx
import wvlet.lang.api.v1.query.Column
import wvlet.lang.api.{StatusCode, WvletLangException}
import wvlet.log.LogSupport

import scala.concurrent.Future
import scala.scalajs.js
import scala.scalajs.js.annotation.*
import Arrow.*

/**
  * A facade for accessing DuckDB.ts
  */
@js.native
@JSGlobal("duckdb")
class DuckDBWasm() extends js.Object:
  def query(sql: String): js.Promise[ArrowTable] = js.native
  def close(): Unit                              = js.native
end DuckDBWasm

class DuckDB() extends AutoCloseable with LogSupport:

  private val db = new DuckDBWasm()

  def query(sql: String): Rx[ArrowTable] =
    import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits.*
    val p = scala.concurrent.Promise[ArrowTable]()
    db.query(sql)
      .`then` { result =>
        p.success(result)
      }
      .`catch` { e =>
        p.failure(StatusCode.QUERY_EXECUTION_FAILURE.newException(s"Query failed:\n${e}"))
      }
    Rx.future(p.future)

  override def close(): Unit = db.close()

end DuckDB

object Arrow extends LogSupport:
  @js.native
  @JSImport("arrow", "Table")
  class ArrowTable extends js.Object:
    def schema: ArrowSchema         = js.native
    def numRows: Int                = js.native
    def toArray(): js.Array[js.Any] = js.native

  end ArrowTable

  private def eval(v: js.Any): Any =
    v match
      case x: js.Array[js.Any] @unchecked =>
        x.toSeq.map(eval)
      case x: js.Any =>
        x

  // Constants for date conversion
  private val MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000L

  private def convertDaysToDateString(days: Double): String =
    val epochMs = (days * MILLISECONDS_PER_DAY).toLong
    new js.Date(epochMs.toDouble).toISOString().split("T")(0)

  private def convertDaysToDateString(days: Int): String =
    val epochMs = days.toLong * MILLISECONDS_PER_DAY
    new js.Date(epochMs.toDouble).toISOString().split("T")(0)

  private def convertMillisToDateString(ms: Double): String =
    new js.Date(ms).toISOString().split("T")(0)

  private def convertMillisToDateString(ms: Long): String =
    new js.Date(ms.toDouble).toISOString().split("T")(0)

  private def formatDateValue(value: Any, typeName: String): Any =
    val normalizedType = typeName.toLowerCase.trim
    normalizedType match
      case tpe if tpe.startsWith("date32") =>
        // Date32 represents days since Unix epoch (1970-01-01)
        value match
          case null => null
          case days: Double =>
            try convertDaysToDateString(days)
            catch case _: Throwable => value
          case days: Int =>
            try convertDaysToDateString(days)
            catch case _: Throwable => value
          case _ => value
      case tpe if tpe.startsWith("date64") =>
        // Date64 represents milliseconds since Unix epoch
        value match
          case null => null
          case ms: Double =>
            try convertMillisToDateString(ms)
            catch case _: Throwable => value
          case ms: Long =>
            try convertMillisToDateString(ms)
            catch case _: Throwable => value
          case _ => value
      case "date" =>
        // Handle DuckDB's native date type which is represented as days since epoch
        value match
          case null => null
          case days: Double =>
            try convertDaysToDateString(days)
            catch case _: Throwable => value
          case days: Int =>
            try convertDaysToDateString(days)
            catch case _: Throwable => value
          case _ => value
      case _ => value

  extension (t: ArrowTable)
    def asScalaArray: Seq[Seq[Any]] = 
      val schema = t.schema
      val fields = schema.fields.toSeq
      val fieldTypes = fields.map(f => f.`type`.toString.toLowerCase)
      
      t.toArray()
        .toSeq
        .map { row =>
          val dict = row.asInstanceOf[js.Dictionary[js.Any]]
          val values = dict.map(_._2).toSeq
          // Process each value: eval() handles nested JS structures, then formatDateValue() converts dates
          values.zip(fieldTypes).map { case (value, typeName) =>
            formatDateValue(eval(value), typeName)
          }
        }

  @js.native
  @JSImport("arrow", "Schema")
  class ArrowSchema extends js.Object:
    def fields: js.Array[ArrowField] = js.native
  end ArrowSchema

  private val decimalTypePattern = """decimal\[([0-9]+)e\+([0-9]+)\]""".r
  extension (s: ArrowSchema)
    def columns: Seq[Column] = s
      .fields
      .toSeq
      .map { f =>
        val tpeName = f.`type`.toString.toLowerCase
        tpeName match
          case decimalTypePattern(p, s) =>
            // Workaround for the weird decimal type representaion in DuckDB Wasm
            Column(f.name, s"decimal(${p},${s})")
          case _ =>
            Column(f.name, tpeName)
      }

  @js.native
  @JSImport("arrow", "Field")
  class ArrowField extends js.Object:
    def name: String   = js.native
    def `type`: js.Any = js.native
  end ArrowField

end Arrow

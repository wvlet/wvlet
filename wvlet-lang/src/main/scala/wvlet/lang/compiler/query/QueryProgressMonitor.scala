package wvlet.lang.compiler.query

trait QueryProgressMonitor extends AutoCloseable:
  def newQuery(sql: String): Unit = {}
  def reportProgress(metric: QueryMetric): Unit
  def close(): Unit = {}

object QueryProgressMonitor:
  def noOp: QueryProgressMonitor =
    new QueryProgressMonitor:
      override def reportProgress(metric: QueryMetric): Unit = {}

trait QueryMetric

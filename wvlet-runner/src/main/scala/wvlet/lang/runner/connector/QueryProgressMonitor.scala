package wvlet.lang.runner.connector

import io.trino.jdbc.QueryStats

trait QueryMetric
object QueryMetric:
  case class TrinoQueryMetric(stats: QueryStats) extends QueryMetric

trait QueryProgressMonitor:
  def newQuery(sql: String): Unit = {}
  def reportProgress(metric: QueryMetric): Unit

object QueryProgressMonitor:
  def noOp: QueryProgressMonitor =
    new QueryProgressMonitor:
      override def reportProgress(metric: QueryMetric): Unit = ()

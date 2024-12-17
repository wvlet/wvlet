package wvlet.lang.runner.connector

import io.trino.jdbc.QueryStats
import wvlet.log.LogSupport
import wvlet.lang.compiler.query.QueryMetric

case class TrinoQueryMetric(stats: QueryStats) extends QueryMetric

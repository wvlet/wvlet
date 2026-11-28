package wvlet.lang.compiler.query

import wvlet.lang.compiler.CompilationUnit

/**
  * Receives progress of one query from the compiler, the connectors, and the in-process engine. The
  * default methods ignore everything, so a monitor overrides only what it records.
  */
trait QueryProgressMonitor extends AutoCloseable:
  def startCompile(unit: CompilationUnit): Unit = {}

  /** A SQL statement sent to a SQL engine (DuckDB, Trino, ...) */
  def newQuery(sql: String): Unit = {}

  def reportProgress(metric: QueryMetric): Unit

  /**
    * The in-process engine planned a statement: `plan` is its physical plan text, evaluated only
    * when a monitor reads it, and `memory` the statement's memory context, readable while and after
    * it runs
    */
  def engineQueryStarted(plan: => String, memory: QueryMemoryUsage): Unit = {}

  def close(): Unit = {}

object QueryProgressMonitor:
  def noOp: QueryProgressMonitor =
    new QueryProgressMonitor:
      override def reportProgress(metric: QueryMetric): Unit = {}

trait QueryMetric

/** The memory one engine statement holds, as its memory context reports it */
trait QueryMemoryUsage:
  /** Whether the statement counts its memory (false with memory accounting off) */
  def isTracking: Boolean

  /** Bytes held now; 0 once the statement has finished */
  def reservedBytes: Long

  /** The most bytes held at once */
  def peakBytes: Long

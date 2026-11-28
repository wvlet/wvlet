package wvlet.lang.server

import wvlet.lang.api.v1.query.QueryMemory
import wvlet.lang.compiler.query.QueryMemoryUsage
import wvlet.lang.compiler.query.QueryMetric
import wvlet.lang.compiler.query.QueryProgressMonitor

/**
  * Records what one server query sent to its engines: SQL statements for SQL engines, physical
  * plans and memory contexts of the in-process engine's statements. The runner reports to it from
  * the query's worker thread while monitoring threads read it, so access is synchronized. Bounded:
  * a script running many statements keeps the first [[QueryRecorder.MaxStatements]] SQL statements
  * and plans, and only the memory contexts still holding memory.
  */
class QueryRecorder extends QueryProgressMonitor:
  private var sqls     = Vector.empty[String]
  private var plans    = Vector.empty[String]
  private var memories = Vector.empty[QueryMemoryUsage]
  // The largest peak of a statement no longer in `memories`
  private var droppedPeak = 0L

  override def reportProgress(metric: QueryMetric): Unit = {}

  override def newQuery(sql: String): Unit = synchronized {
    if sqls.size < QueryRecorder.MaxStatements then
      sqls :+= sql
  }

  override def engineQueryStarted(plan: => String, memory: QueryMemoryUsage): Unit =
    val text =
      if plansFull then
        None
      else
        Some(plan)
    synchronized {
      text.foreach(t =>
        if plans.size < QueryRecorder.MaxStatements then
          plans :+= t
      )
      if memory.isTracking then
        // A query's statements run one after another, so earlier contexts holding nothing have
        // finished: fold them into the dropped peak and keep only those still holding memory
        // (statements an engine runs inside another one)
        val (done, open) = memories.partition(_.reservedBytes == 0L)
        droppedPeak = (droppedPeak +: done.map(_.peakBytes)).max
        memories = open :+ memory
    }

  private def plansFull: Boolean = synchronized(plans.size >= QueryRecorder.MaxStatements)

  def sql: Seq[String] = synchronized(sqls)

  def enginePlans: Seq[String] = synchronized(plans)

  /** Bytes held now, and the largest peak of one statement */
  def memory: QueryMemory = synchronized {
    if memories.isEmpty && droppedPeak == 0L then
      QueryMemory()
    else
      QueryMemory(
        tracked = true,
        reservedBytes = memories.map(_.reservedBytes).sum,
        peakBytes = (droppedPeak +: memories.map(_.peakBytes)).max
      )
  }

end QueryRecorder

object QueryRecorder:
  /** SQL statements and engine plans a recorder keeps */
  val MaxStatements: Int = 100

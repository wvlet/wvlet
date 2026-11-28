package wvlet.lang.engine

import com.sun.management.GarbageCollectionNotificationInfo

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicLong
import javax.management.Notification
import javax.management.NotificationEmitter
import javax.management.NotificationListener
import javax.management.openmbean.CompositeData
import scala.jdk.CollectionConverters.*

/**
  * Approximates the live heap of a benchmark run: the largest heap used after any garbage
  * collection that ends while the probe is open. Garbage not collected yet (an old generation
  * waiting for a mixed collection) biases it upwards; a run with no collection reports nothing.
  *
  * {{{
  *   val probe = LiveHeapProbe.start()
  *   try run() finally probe.stop()
  *   probe.maxMb // -1 when no collection ended during the run
  * }}}
  */
class LiveHeapProbe private ():
  private val maxBytes = AtomicLong(-1L)

  private val listener: NotificationListener =
    (n: Notification, _: AnyRef) =>
      if n.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION then
        val info = GarbageCollectionNotificationInfo.from(n.getUserData.asInstanceOf[CompositeData])
        val used = info.getGcInfo.getMemoryUsageAfterGc.values().asScala.map(_.getUsed).sum
        maxBytes.accumulateAndGet(used, math.max)

  private def emitters: Seq[NotificationEmitter] = ManagementFactory
    .getGarbageCollectorMXBeans
    .asScala
    .toSeq
    .collect { case e: NotificationEmitter =>
      e
    }

  private def register(): Unit = emitters.foreach(_.addNotificationListener(listener, null, null))

  /** Stops listening; GC notifications arrive asynchronously, so late ones may be missed */
  def stop(): Unit = emitters.foreach(e =>
    try
      e.removeNotificationListener(listener)
    catch
      case _: javax.management.ListenerNotFoundException =>
        ()
  )

  /** The largest heap used after a collection in MB, or -1 when no collection ended */
  def maxMb: Double =
    val b = maxBytes.get()
    if b < 0 then
      -1.0
    else
      b / 1048576.0

end LiveHeapProbe

object LiveHeapProbe:
  /** Starts listening to the JVM's garbage collection notifications */
  def start(): LiveHeapProbe =
    val p = LiveHeapProbe()
    p.register()
    p

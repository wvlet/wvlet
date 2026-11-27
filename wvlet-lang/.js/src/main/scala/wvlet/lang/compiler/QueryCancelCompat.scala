package wvlet.lang.compiler

import scala.scalajs.js

/**
  * The cancel flag on Node.js, where a query blocks the event loop so no SIGINT listener can run
  * until it ends. The flag lives in a `SharedArrayBuffer`, so a watcher on another thread (the
  * REPL's Ctrl-C watcher) can raise it while the query runs, and interrupt the DuckDB connection
  * the query is waiting on.
  *
  * Layout: Int32 slot 0 is the cancel request, slot 1 is whether a cancellable query is running
  * (armed), slot 2 is whether the watcher is ready; the BigInt64 at byte 16 is the address of the
  * DuckDB connection running a query, or 0.
  */
trait QueryCancelCompat:
  private val Requested = 0
  private val Armed     = 1

  private lazy val buffer: js.Dynamic =
    js.Dynamic.newInstance(js.Dynamic.global.SharedArrayBuffer)(24)

  private lazy val slots: js.Dynamic = js.Dynamic.newInstance(js.Dynamic.global.Int32Array)(buffer)
  private lazy val connection: js.Dynamic =
    js.Dynamic.newInstance(js.Dynamic.global.BigInt64Array)(buffer, 16, 1)

  private def atomics: js.Dynamic = js.Dynamic.global.Atomics

  private def load(slot: Int): Int = atomics.load(slots, slot).asInstanceOf[Int]

  /** The libduckdb path DuckDB was loaded from, for the watcher's own `duckdb_interrupt` binding */
  private var duckdbLibraryPath: Option[String]            = None
  private var duckdbLibraryListeners: List[String => Unit] = Nil

  /** Whether the user asked to cancel the running query */
  def isRequested: Boolean = load(Requested) == 1 && load(Armed) == 1

  /** The shared buffer that holds the flag, to hand to the watcher thread */
  def sharedBuffer: js.Any = buffer

  /** Start a cancellable query: clear any earlier request and accept new ones */
  def arm(): Unit =
    atomics.store(slots, Requested, 0)
    atomics.store(slots, Armed, 1)

  /**
    * End the cancellable query. Returns whether the user asked to cancel it, and clears the request
    * so that later work isn't cancelled by it
    */
  def disarm(): Boolean =
    val requested = isRequested
    atomics.store(slots, Armed, 0)
    atomics.store(slots, Requested, 0)
    requested

  /** Run a DuckDB call, letting the watcher interrupt the connection at `address` meanwhile */
  def withDuckDBConnection[A](address: js.Any)(body: => A): A =
    atomics.store(connection, 0, address)
    try body
    finally atomics.store(connection, 0, js.BigInt(0))

  /**
    * Call `f` with the libduckdb path once DuckDB is loaded, at once if it already is. DuckDB loads
    * lazily, often inside the first query, so the watcher learns the path from this callback
    */
  def onDuckDBLibraryLoaded(f: String => Unit): Unit =
    duckdbLibraryPath match
      case Some(p) =>
        f(p)
      case None =>
        duckdbLibraryListeners = f :: duckdbLibraryListeners

  /** Record the libduckdb path DuckDB was loaded from */
  def setDuckDBLibraryPath(path: String): Unit =
    duckdbLibraryPath = Some(path)
    duckdbLibraryListeners.foreach(_(path))
    duckdbLibraryListeners = Nil

end QueryCancelCompat

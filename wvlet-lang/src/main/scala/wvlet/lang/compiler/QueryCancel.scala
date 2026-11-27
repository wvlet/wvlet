package wvlet.lang.compiler

import wvlet.lang.api.StatusCode

/**
  * Cooperative cancellation of the running query. Long-running loops (engine batch pulls, Trino
  * pages, DuckDB calls on Node.js) call [[check]] so that Ctrl-C in the REPL can stop a query that
  * runs on the caller's thread.
  *
  * Only Node.js raises the flag (see the JS [[QueryCancelCompat]]). The JVM and Native run REPL
  * queries on a thread and cancel them through the thread, so [[isRequested]] is a constant `false`
  * there and [[check]] costs nothing.
  */
object QueryCancel extends QueryCancelCompat:

  /** Throw a QUERY_CANCELLED error if the user asked to cancel the running query */
  def check(): Unit =
    if isRequested then
      throw cancelled()

  /** The error that a cancelled query ends with */
  def cancelled(): Throwable = StatusCode.QUERY_CANCELLED.newException("The query was cancelled")

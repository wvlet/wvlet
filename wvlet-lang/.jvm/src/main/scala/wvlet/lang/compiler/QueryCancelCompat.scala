package wvlet.lang.compiler

/**
  * Queries are cancelled through their thread on this platform, so the cooperative flag is never
  * raised
  */
trait QueryCancelCompat:
  /** Whether the user asked to cancel the running query. Always false here */
  final def isRequested: Boolean = false

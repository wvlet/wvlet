package wvlet.lang.api.v1.query

enum QueryStatus(val isFinished: Boolean, val isFailed: Boolean):
  case STARTING extends QueryStatus(isFinished = false, isFailed = false)
  case QUEUED   extends QueryStatus(isFinished = false, isFailed = false)
  case RUNNING  extends QueryStatus(isFinished = false, isFailed = false)
  case FINISHED extends QueryStatus(isFinished = true, isFailed = false)
  case FAILED   extends QueryStatus(isFinished = true, isFailed = true)
  case CANCELED extends QueryStatus(isFinished = true, isFailed = true)

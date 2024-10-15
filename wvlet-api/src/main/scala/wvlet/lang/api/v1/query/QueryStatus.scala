package wvlet.lang.api.v1.query

enum QueryStatus(val isFinished: Boolean):
  case STARTING extends QueryStatus(isFinished = false)
  case QUEUED   extends QueryStatus(isFinished = false)
  case RUNNING  extends QueryStatus(isFinished = false)
  case FINISHED extends QueryStatus(isFinished = true)
  case FAILED   extends QueryStatus(isFinished = true)
  case CANCELED extends QueryStatus(isFinished = true)

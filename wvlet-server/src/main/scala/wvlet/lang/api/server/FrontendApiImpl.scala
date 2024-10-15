package wvlet.lang.api.server

import wvlet.airframe.metrics.ElapsedTime
import wvlet.airframe.ulid.ULID
import wvlet.lang.api.v1.frontend.FrontendApi
import wvlet.lang.api.v1.frontend.FrontendApi.*
import wvlet.lang.api.v1.query.QueryInfo

class FrontendApiImpl() extends FrontendApi:
  private val startTimeNs = System.nanoTime()

  override def status: ServerStatus = ServerStatus(upTime = ElapsedTime.nanosSince(startTimeNs))

  override def submitQuery(request: QueryRequest): QueryResponse = ???

  override def getQueryInfo(request: QueryInfoRequest): QueryInfo = ???

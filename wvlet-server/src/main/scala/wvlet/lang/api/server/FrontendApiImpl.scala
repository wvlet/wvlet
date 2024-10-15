package wvlet.lang.api.server

import wvlet.airframe.metrics.ElapsedTime
import wvlet.airframe.ulid.ULID
import wvlet.lang.api.v1.frontend.FrontendApi
import wvlet.lang.api.v1.frontend.FrontendApi.*
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.log.LogSupport

class FrontendApiImpl() extends FrontendApi with LogSupport:
  private val startTimeNs = System.nanoTime()

  override def status: ServerStatus = ServerStatus(upTime = ElapsedTime.nanosSince(startTimeNs))

  override def submitQuery(request: QueryRequest): QueryResponse =
    debug(request)
    QueryResponse(queryId = ULID.newULID, requestId = request.requestId)

  override def getQueryInfo(request: QueryInfoRequest): QueryInfo = ???

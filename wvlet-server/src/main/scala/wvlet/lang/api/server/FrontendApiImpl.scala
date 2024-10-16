package wvlet.lang.api.server

import wvlet.airframe.metrics.ElapsedTime
import wvlet.airframe.ulid.ULID
import wvlet.lang.api.v1.frontend.FrontendApi
import wvlet.lang.api.v1.frontend.FrontendApi.*
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.log.LogSupport

class FrontendApiImpl(queryService: QueryService) extends FrontendApi with LogSupport:
  private val startTimeNs = System.nanoTime()

  override def status: ServerStatus = ServerStatus(upTime = ElapsedTime.nanosSince(startTimeNs))

  override def submitQuery(request: QueryRequest): QueryResponse =
    debug(s"Received:\n${request}")
    val resp = queryService.enqueue(request)
    resp

  override def getQueryInfo(request: QueryInfoRequest): QueryInfo =
    debug(request)
    queryService.fetchNext(request)

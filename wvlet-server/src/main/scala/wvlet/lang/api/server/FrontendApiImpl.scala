package wvlet.lang.api.server

import wvlet.airframe.ulid.ULID
import wvlet.lang.api.v1.frontend.FrontendApi
import wvlet.lang.api.v1.frontend.FrontendApi.*
import wvlet.lang.api.v1.query.QueryInfo

class FrontendApiImpl extends FrontendApi:
  override def submitQuery(request: QueryRequest): QueryResponse = ???

  override def getQueryInfo(request: QueryInfoRequest): QueryInfo = ???

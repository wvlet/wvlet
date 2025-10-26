package wvlet.lang.ui.playground

import wvlet.airframe.rx.Rx
import wvlet.airframe.rx.RxVar
import wvlet.lang.api.v1.query.QueryRequest
import wvlet.lang.api.v1.query.QueryResult

class CurrentQuery:
  val queryName: RxVar[String]               = Rx.variable("sample.wv")
  val wvletQueryRequest: RxVar[QueryRequest] = Rx.variable(QueryRequest(query = "select 1"))
  val lastQueryResult: RxVar[QueryResult]    = Rx.variable(QueryResult(Seq.empty, Seq.empty))

  def setQuery(demoQuerySet: DemoQuery): Unit =
    queryName         := demoQuerySet.name
    wvletQueryRequest := QueryRequest(query = demoQuerySet.query)

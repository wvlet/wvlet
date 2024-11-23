package wvlet.lang.ui.playground

import wvlet.airframe.rx.{Rx, RxVar}
import wvlet.lang.api.v1.query.QueryResult

class CurrentQuery:
  val queryName: RxVar[String]            = Rx.variable("sample.wv")
  val wvletQuery: RxVar[String]           = Rx.variable("""from lineitem""")
  val lastQueryResult: RxVar[QueryResult] = Rx.variable(QueryResult(Seq.empty, Seq.empty))

  def setQuery(demoQuerySet: DemoQuery): Unit =
    queryName  := demoQuerySet.name
    wvletQuery := demoQuerySet.query

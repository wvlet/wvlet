package wvlet.lang.ui.playground

case class DemoQuery(name: String, query: String)
case class DemoQuerySet(connector: String, name: String, queries: List[DemoQuery])

object DemoQuerySet:
  def demoQuerySet: List[DemoQuerySet] = List(DemoQuerySet("tpch", "Examples", defaultQuerySet))

  def defaultQuerySet: List[DemoQuery] =
    SampleQuery
      .allFiles
      .map { (path, query) =>
        DemoQuery(path, query)
      }
      .toList

end DemoQuerySet

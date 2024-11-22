package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*

import scala.collection.immutable.ListMap

class QuerySetSelector(currentQuery: CurrentQuery, queryEditor: QueryEditor) extends RxElement:
  override def render =
    def separator(): RxElement = div(cls -> "border-t border-gray-600 mt-2 mb-2")
    div(
      cls -> "h-full bg-slate-700 p-3 text-sm text-slate-200",
      h2("Playground"),
      separator(),
      DemoQuerySet
        .demoQueries
        .map { (category, queries) =>
          div(
            h2(cls -> "text-sm font-bold text-slate-300", category),
            ul(
              queries.map { q =>
                li(
                  cls -> "text-slate-400 hover:text-white px-2",
                  a(
                    href -> "#",
                    q.name,
                    onclick -> { e =>
                      e.preventDefault()
                      currentQuery.queryName  := q.name
                      currentQuery.wvletQuery := q.query
                      queryEditor.setText(q.query)
                    }
                  )
                )
              }
            )
          )
        },
      separator()
    )

  end render

end QuerySetSelector

case class DemoQuery(name: String, query: String)

object DemoQuerySet:
  def demoQueries: ListMap[String, List[DemoQuery]] = ListMap("Examples" -> querySet)

  def querySet: List[DemoQuery] = List(
    DemoQuery(
      "sample.wv",
      """-- scan from a file
          |from lineitem
          |-- add filtering condition
          |where l_quantity > 0.0
          |-- grouping by keys
          |group by l_returnflag, l_linestatus
          |-- add aggregation exprs
          |agg
          |  l_quantity.sum as sum_qty,
          |  l_extendedprice.sum as sum_ext_price
          |-- remove unnecessary column
          |exclude l_returnflag
          |-- add ordering
          |order by sum_qty desc
          |""".stripMargin
    ),
    DemoQuery("limit.wv", "from lineitem\nlimit 10"),
    DemoQuery("file3.wv", "from lineitem")
  )

package wvlet.lang.ui.playground

import wvlet.airframe.rx.{Rx, RxVar}

class CurrentQuery:
  val queryName: RxVar[String] = Rx.variable("sample.wv")
  val wvletQuery: RxVar[String] = Rx.variable("""-- scan from a file
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
        |""".stripMargin)

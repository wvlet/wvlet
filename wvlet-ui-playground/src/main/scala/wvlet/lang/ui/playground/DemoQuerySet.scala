package wvlet.lang.ui.playground

case class DemoQuery(name: String, query: String)
case class DemoQuerySet(connector: String, name: String, queries: List[DemoQuery])

object DemoQuerySet:
  def demoQueries: List[DemoQuerySet] = List(DemoQuerySet("tpch", "Examples", querySet))

  def querySet: List[DemoQuery] = List(
    DemoQuery(
      "sample.wv",
      """-- Acan a table
          |from lineitem
          |-- Add filtering condition
          |where l_quantity > 0.0
          |-- Grouping by keys
          |group by l_returnflag, l_linestatus
          |-- Add aggregation expressions
          |agg
          |  l_quantity.sum as sum_qty,
          |  l_extendedprice.sum as sum_ext_price
          |-- Remove unnecessary column
          |exclude l_returnflag
          |-- Sort
          |order by sum_qty desc
          |""".stripMargin
    ),
    DemoQuery(
      "list-tables.wv",
      """-- List tables
        |show tables
        |""".stripMargin
    ),
    DemoQuery(
      "list-schemas.wv",
      """-- List scemas
        |from wv_schemas
        |""".stripMargin
    ),
    DemoQuery(
      "scan.wv",
      """-- Simple table scan
        |from lineitem
        |-- Add a condition
        |where l_returnflag = 'R'
        |-- Take 10 rows
        |limit 10
        |""".stripMargin
    ),
    DemoQuery(
      "model.wv",
      """-- Define a new model
        |model my_query =
        |  from lineitem
        |  where l_quantity > 10.0
        |end
        |
        |-- Refer to the model
        |from my_query
        |-- Add a further condition
        |where l_returnflag = 'N'
        |limit 5
        |""".stripMargin
    ),
    DemoQuery(
      "model_with_param.wv",
      s"""
        |-- Define a model with parameters
        |model my_query(p1: double, p2: string) =
        |  from lineitem
        |  where l_quantity > p1
        |  where l_returnflag = p2
        |end
        |
        |-- Use the model with parameters
        |from my_query(5, 'R')
        |limit 5
        |
        |-- Use the model with different parameters
        |from my_query(20, 'N')
        |limit 5
        |""".stripMargin
    ),
    DemoQuery(
      "tpch-q1.wv",
      """-- TPCH-H q1
        |from lineitem
        |where l_shipdate <= '1998-09-02'.to_date
        |group by l_returnflag, l_linestatus
        |select
        |  l_returnflag,
        |  l_linestatus,
        |  sum_qty        = l_quantity.sum,
        |  sum_base_price = l_extendedprice.sum,
        |  sum_disc_price = (l_extendedprice * (1 - l_discount)).sum,
        |  sum_charge     = (l_extendedprice * (1 - l_discount) * (1 + l_tax)).sum,
        |  avg_qty        = l_quantity.avg,
        |  avg_price      = l_extendedprice.avg,
        |  avg_disc       = l_discount.avg,
        |  count_order    = _.count
        |order by
        |  l_returnflag,
        |  l_linestatus
        |""".stripMargin
    ),
    DemoQuery(
      "tpch-q3.wv",
      """-- TPC-H q3
        |from
        |    customer,
        |    orders,
        |    lineitem
        |where
        |    c_mktsegment = 'BUILDING'
        |    and c_custkey = o_custkey
        |    and l_orderkey = o_orderkey
        |    and o_orderdate < '1995-03-15'.to_date
        |    and l_shipdate > '1995-03-15'.to_date
        |group by
        |    l_orderkey,
        |    o_orderdate,
        |    o_shippriority
        |select
        |    l_orderkey,
        |    revenue = (l_extendedprice * (1 - l_discount)).sum,
        |    o_orderdate,
        |    o_shippriority
        |order by
        |    revenue desc,
        |    o_orderdate
        |limit 10
        |""".stripMargin
    )
  )

end DemoQuerySet

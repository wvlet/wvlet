/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.cli

import wvlet.airspec.AirSpec

class WvletREPLTest extends AirSpec:
  test("help") {
    WvletREPLCli.main("-c 'help'")
  }

  test("model in the working folder") {
    WvletREPLCli.main("-w spec/model1 -c 'from person_filter(2)'")
  }

  test("def new model") {
    WvletREPLCli
      .main("-w spec/model1 -c 'model m(v:int) = from person where id = v end' -c 'from m(1)'")
  }

  test("limit shown rows") {
    WvletREPLCli
      .main("""-c "from 'https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'" """)
  }

  test("show models") {
    WvletREPLCli.main("-w spec/model1 -c 'show models'")
    WvletREPLCli.main("-w spec/model1 -c 'show models limit 1'")
    WvletREPLCli.main("-w spec/model1 -c 'show models' -c 'show models limit 5'")
  }

  test("select group index") {
    WvletREPLCli.main("-w spec/model1 -c 'from person group by age / 10 select _1'")
  }

  test("clip") {
    WvletREPLCli.main("-w spec/model1 -c 'from person' -c 'clip'")
  }

  test("clip-result") {
    WvletREPLCli.main("-w spec/model1 -c 'from person' -c 'clip-result'")
  }

  test("rows") {
    WvletREPLCli.main("-w spec/model1 -c 'rows 2' -c 'from person'")
  }

  test("col-width") {
    WvletREPLCli.main(
      """-c 'col-width 10' -c "from 'https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'""""
    )
  }

end WvletREPLTest
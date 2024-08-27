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
package wvlet.lang.spec

import wvlet.lang.runner.cli.{WvletCli, WvletREPLCli}
import wvlet.airspec.AirSpec

class TDTrinoTest extends AirSpec:
  if inCI then
    skip("Trino td-dev profile is not available in CI")

  test("resolve string") {
    WvletREPLCli.main("""--profile td-dev -c 'from accounts where data_center.in("aws")'""")
  }

  test("show tables") {
    WvletREPLCli.main("--profile td-dev -c 'show tables'")
  }

  test("time.within") {
    WvletREPLCli.main("""--profile td-dev -w spec/trino --file within.wv""")
  }

  test("sfdc.account") {
    WvletREPLCli.main("""--profile td-dev -w spec/trino --file sfdc_accounts.wv""")
  }

  test("pivot") {
    WvletREPLCli.main("""--profile td-dev -w spec/trino --file pivot.wv""")
  }

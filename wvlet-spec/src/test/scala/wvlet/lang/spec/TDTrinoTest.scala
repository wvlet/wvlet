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

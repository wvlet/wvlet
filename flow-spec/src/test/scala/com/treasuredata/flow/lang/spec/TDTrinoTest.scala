package com.treasuredata.flow.lang.spec

import com.treasuredata.flow.lang.runner.cli.{FlowCli, FlowREPLCli}
import wvlet.airspec.AirSpec

class TDTrinoTest extends AirSpec:
  if inCI then
    skip("Trino td-dev profile is not available in CI")

  test("resolve string") {
    FlowREPLCli.main("""--profile td-dev -c 'from accounts where data_center.in("aws")'""")
  }

  test("show tables") {
    FlowREPLCli.main("--profile td-dev -c 'show tables'")
  }

  test("time.within") {
    FlowREPLCli.main("""--profile td-dev -w spec/trino --file within.flow""")
  }

  test("sfdc.account") {
    FlowREPLCli.main("""--profile td-dev -w spec/trino --file sfdc_accounts.flow""")
  }

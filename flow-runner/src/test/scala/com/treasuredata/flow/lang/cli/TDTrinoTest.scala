package com.treasuredata.flow.lang.cli

import wvlet.airspec.AirSpec

class TDTrinoTest extends AirSpec:
  if inCI then
    skip("Trino td-dev profile is not available in CI")

  test("resolve string") {
    FlowREPLCli.main("""--profile td-dev -c 'from accounts where data_center.in("aws")'""")
  }

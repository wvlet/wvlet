package wvlet.lang.runner

class SqlTPCHSpec extends SpecRunner("spec/sql/tpc-h", parseOnly = true, prepareTPCH = true)

class SqlTPCDSSpec extends SpecRunner("spec/sql/tpc-ds", parseOnly = true, prepareTPCDS = true)

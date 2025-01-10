package wvlet.lang.runner

class SQLTPCHSpec extends SpecRunner("spec/sql/tpc-h", prepareTPCH = true)

class SQLTPCDSSpec extends SpecRunner("spec/sql/tpc-ds", prepareTPCDS = true)

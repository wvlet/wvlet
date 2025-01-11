package wvlet.lang.runner

class SqlTPCHSpec extends SpecRunner("spec/sql/tpc-h", prepareTPCH = true)

class SqlTPCDSSpec extends SpecRunner("spec/sql/tpc-ds", prepareTPCDS = true):
  pending("TPC-DS is not fully supported yet")
end SqlTPCDSSpec

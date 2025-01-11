package wvlet.lang.runner

class TPCHSqlSpec extends SpecRunner("spec/sql/tpc-h", prepareTPCH = true)

class TPCDSSqlSpec extends SpecRunner("spec/sql/tpc-ds", prepareTPCDS = true):
  pending("TPC-DS is not fully supported yet")
end TPCDSSqlSpec

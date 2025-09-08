package wvlet.lang.runner

class SqlBasicSpec
    extends SpecRunner(
      "spec/sql/basic",
      ignoredSpec = Map(
        "show-create-view.sql" -> "SHOW CREATE VIEW execution not yet fully supported"
      )
    )

class SqlTPCHSpec extends SpecRunner("spec/sql/tpc-h", parseOnly = true, prepareTPCH = true)

class SqlTPCDSSpec extends SpecRunner("spec/sql/tpc-ds", parseOnly = true, prepareTPCDS = true)

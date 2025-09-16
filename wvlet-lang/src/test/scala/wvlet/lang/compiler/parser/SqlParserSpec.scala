package wvlet.lang.compiler.parser

// Basic SQL queries
class SqlParserSpecBasic extends ParserSpec("spec/sql/basic")
// TPC-H SQL
class SqlParserSpecTPCH  extends ParserSpec("spec/sql/tpc-h")
// TPC-DS SQL
class SqlParserSpecTPCDS extends ParserSpec("spec/sql/tpc-ds")

// Trino SQL
class SqlParserSpecTrino  extends ParserSpec("spec/sql/trino")

// Hive SQL
class SqlParserSpecHive
    extends ParserSpec(
      "spec/sql/hive",
      Map(
        "hive-data-types.sql" -> "Temporarily ignored - complex Hive data types not yet supported"
      )
    )

// Update SQL
class SqlParserSpecUpdate extends ParserSpec("spec/sql/update")

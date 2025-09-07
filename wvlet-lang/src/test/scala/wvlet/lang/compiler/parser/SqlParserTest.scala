package wvlet.lang.compiler.parser

import wvlet.airspec.AirSpec
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.model.plan.*

class SqlParserTest extends AirSpec:
  test("parse") {
    val stmt = SqlParser(CompilationUnit.fromSqlString("select * from A")).parse()
    debug(stmt.pp)
  }

  test("ALTER TABLE syntax") {
    // Test basic ALTER TABLE RENAME TO
    val renameStmt = SqlParser(
      CompilationUnit.fromSqlString("ALTER TABLE users RENAME TO customers")
    ).parse()
    debug(s"RENAME: ${renameStmt.pp}")

    // Test ALTER TABLE ADD COLUMN
    val addStmt = SqlParser(
      CompilationUnit.fromSqlString("ALTER TABLE users ADD COLUMN email VARCHAR")
    ).parse()
    debug(s"ADD COLUMN: ${addStmt.pp}")

    // Test ALTER TABLE DROP COLUMN
    val dropStmt = SqlParser(CompilationUnit.fromSqlString("ALTER TABLE users DROP COLUMN email"))
      .parse()
    debug(s"DROP COLUMN: ${dropStmt.pp}")

    // Test ALTER TABLE ALTER COLUMN TYPE (DuckDB syntax)
    val alterTypeStmt = SqlParser(
      CompilationUnit.fromSqlString("ALTER TABLE users ALTER age TYPE VARCHAR")
    ).parse()
    debug(s"ALTER TYPE: ${alterTypeStmt.pp}")

    // Test ALTER TABLE SET AUTHORIZATION
    val authStmt = SqlParser(
      CompilationUnit.fromSqlString("ALTER TABLE users SET AUTHORIZATION (USER admin)")
    ).parse()
    debug(s"SET AUTHORIZATION: ${authStmt.pp}")

    // Test more complex ALTER TABLE statements from the requirements

    // Trino-style ALTER TABLE ADD COLUMN with options
    val addColumnWithComment = SqlParser(
      CompilationUnit.fromSqlString(
        "ALTER TABLE users ADD COLUMN description VARCHAR NOT NULL COMMENT 'User description'"
      )
    ).parse()
    debug(s"ADD COLUMN WITH COMMENT: ${addColumnWithComment.pp}")

    // ALTER TABLE RENAME COLUMN
    val renameColumn = SqlParser(
      CompilationUnit.fromSqlString("ALTER TABLE users RENAME COLUMN old_name TO new_name")
    ).parse()
    debug(s"RENAME COLUMN: ${renameColumn.pp}")

    // ALTER TABLE ALTER COLUMN SET DATA TYPE (Standard SQL)
    val setDataType = SqlParser(
      CompilationUnit.fromSqlString("ALTER TABLE users ALTER COLUMN age SET DATA TYPE INTEGER")
    ).parse()
    debug(s"SET DATA TYPE: ${setDataType.pp}")

    // ALTER TABLE ALTER COLUMN SET DATA TYPE with USING (DuckDB)
    val setDataTypeWithUsing = SqlParser(
      CompilationUnit.fromSqlString(
        "ALTER TABLE users ALTER COLUMN i SET DATA TYPE VARCHAR USING concat(i, '_', j)"
      )
    ).parse()
    debug(s"SET DATA TYPE WITH USING: ${setDataTypeWithUsing.pp}")

    // ALTER TABLE ALTER COLUMN DROP NOT NULL
    val dropNotNull = SqlParser(
      CompilationUnit.fromSqlString("ALTER TABLE users ALTER COLUMN age DROP NOT NULL")
    ).parse()
    debug(s"DROP NOT NULL: ${dropNotNull.pp}")

    // ALTER TABLE SET PROPERTIES
    val setProperties = SqlParser(
      CompilationUnit.fromSqlString(
        "ALTER TABLE users SET PROPERTIES format = 'parquet', compression = 'gzip'"
      )
    ).parse()
    debug(s"SET PROPERTIES: ${setProperties.pp}")

    // ALTER TABLE EXECUTE
    val execute = SqlParser(
      CompilationUnit.fromSqlString(
        "ALTER TABLE users EXECUTE optimize (max_file_size = 1000000) WHERE created_at < '2023-01-01'"
      )
    ).parse()
    debug(s"EXECUTE: ${execute.pp}")

    // Verify all parsed successfully (no exceptions)
    info("All ALTER TABLE statements from Trino and DuckDB documentation parsed successfully")
  }

end SqlParserTest

class SqlParserTPCHSpec extends AirSpec:
  CompilationUnit
    .fromPath("spec/sql/tpc-h")
    .foreach { unit =>
      test(s"parse tpc-h ${unit.sourceFile.fileName}") {
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        debug(stmt.pp)
      }
    }

class SqlParserTPCDSSpec extends AirSpec:
  CompilationUnit
    .fromPath("spec/sql/tpc-ds")
    .foreach { unit =>
      test(s"parse tpc-ds ${unit.sourceFile.fileName}") {
        val stmt = SqlParser(unit, isContextUnit = true).parse()
        debug(stmt.pp)
      }
    }

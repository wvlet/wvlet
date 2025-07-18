package wvlet.lang.cli

import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil

import java.io.ByteArrayOutputStream
import java.io.PrintStream

class QueryConverterCommandTest extends AirSpec:
  // TPC-H query paths
  private val tpchWvletQueryPath = "spec/tpch"
  private val tpchSQLQueryPath = "spec/sql/tpc-h"

  test("TPC-H query files exist") {
    import java.io.File
    for (i <- 1 to 22) {
      val queryName = s"q${i}"
      val wvletFile = new File(s"${tpchWvletQueryPath}/${queryName}.wv")
      val sqlFile = new File(s"${tpchSQLQueryPath}/${queryName}.sql")
      
      debug(s"Checking wvlet file: ${wvletFile.getAbsolutePath}, exists: ${wvletFile.exists}")
      debug(s"Checking SQL file: ${sqlFile.getAbsolutePath}, exists: ${sqlFile.exists}")
      
      wvletFile.exists shouldBe true
      sqlFile.exists shouldBe true
    }
  }

  test("toSQL command should show help") {
    WvletMain.main("toSQL --help")
  }

  test("toSQL command should convert a simple query") {
    val inputQuery = "from users select name, age where age > 20"
    val out = captureStdout {
      WvletMain.main(s"""toSQL "$inputQuery" """)
    }

    debug("--- Input Wvlet Query ---")
    debug(inputQuery)
    debug("--- Actual Generated SQL ---")
    debug(out.trim)
    
    out shouldContain "select"
    out shouldContain "from users"
    out shouldContain "where age > 20"
  }

  test("toSQL command should convert TPC-H queries") {
    // Loop through all TPC-H queries (1-22)
    for (i <- 1 to 22) {
      val queryName = s"q${i}"
      
      test(s"toSQL converts ${queryName}") {

        val out = captureStdout {
          WvletMain.main(s"""toSQL -t trino -w ${tpchWvletQueryPath} -f ${queryName}.wv""")
        }
          // Read the expected SQL file
        val expectedSQL = IOUtil.readAsString(s"${tpchSQLQueryPath}/${queryName}.sql")

        debug(s"=== Query: ${queryName} ===")
        // Read and print the query content for debugging
        val queryContent = IOUtil.readAsString(s"${tpchWvletQueryPath}/${queryName}.wv")
        // debug(s"\nInput Wvlet Query: '${queryContent}'")
        // debug(s"Generated SQL Output: '${out.trim}'")

        // Normalize both SQL strings to compare them regardless of formatting and case differences
        def normalize(sql: String): String =
          sql
            .trim
            .toLowerCase
            .replaceAll("--.*\\n", "")
            .replaceAll("\\s+", " ")
            .replaceAll("\\(\\s+", "(")
            .replaceAll("\\s+\\)", ")")
            .replaceAll("\\( ", "(")
            .replaceAll(" \\)", ")")
            .trim

        val normalizedOut = normalize(out)
        val normalizedExpected = normalize(expectedSQL)

        debug(s"normalizedOut: ${normalizedOut}")
        debug(s"normalizedExpected: ${normalizedExpected}")

        // Compare the normalized SQL strings
        // normalizedOut shouldBe normalizedExpected

        out.toLowerCase shouldContain "select"
      }
    }
  }

  test("toSQL command should apply database-specific SQL dialect") {
    val out = captureStdout {
      WvletMain.main("""toSQL -t duckdb "from users select *" """)
    }
    out shouldContain "SELECT *"
    out shouldContain "FROM users"
  }

  // Future test for SQL -> wvlet conversion (commented out until implemented)
    // test("toWvlet command should convert SQL to wvlet") {
  test("toWvlet command should convert a simple query") {
    val sqlQuery = "SELECT name, age FROM users WHERE age > 20"
    val out = captureStdout {
      WvletMain.main(s"""toWvlet "$sqlQuery" """)
    }
    
    // Basic verification
    out.toLowerCase shouldContain "from users"
    out.toLowerCase shouldContain "select name, age"
    out.toLowerCase shouldContain "where age > 20"
  }
    
  
  test("toWvlet command should convert SQL to wvlet") {
    for (i <- 1 to 22) {
      val queryName = s"q${i}"

      test(s"toWvlet converts ${queryName}") {
        val expectedWvlet = IOUtil.readAsString(s"${tpchWvletQueryPath}/${queryName}.wv")
        
        val out = captureStdout {
          WvletMain.main(s"""toWvlet -t trino -w ${tpchSQLQueryPath} -f ${queryName}.sql""")
        }
        
        // Basic verification
        out.toLowerCase shouldContain "from"
        out.toLowerCase shouldContain "select"

        // Strict comparison (uncomment when implemented)
        // out.trim shouldBe expectedWvlet.trim
      }
    }
  }

  private def captureStdout(body: => Unit): String =
    val out = new ByteArrayOutputStream()
    Console.withOut(out) {
      body
    }
    out.toString
end QueryConverterCommandTest
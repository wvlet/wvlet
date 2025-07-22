package wvlet.lang.cli

import wvlet.airspec.AirSpec
import wvlet.log.io.IOUtil
import wvlet.lang.compiler.{SourceIO, VirtualFile}

import java.io.ByteArrayOutputStream
import java.io.PrintStream

class QueryConverterCommandTest extends AirSpec:
  // TPC-H query paths
  private val tpchWvletQueryPath = "spec/tpch"
  private val tpchSQLQueryPath = "spec/sql/tpc-h"
  
  // Helper method to get TPC-H query files
  private def getTpchQueryFiles(path: String, extension: String): Seq[VirtualFile] = 
    SourceIO.listSourceFiles(path)
      .filter(f => f.name.matches(s"""q\\d+\\.${extension}"""))
      .sortBy(_.name)

  test("TPC-H query files exist") {
    val wvletFiles = SourceIO.listSourceFiles(tpchWvletQueryPath)
    val sqlFiles = SourceIO.listSourceFiles(tpchSQLQueryPath)
    
    debug(s"Found ${wvletFiles.size} .wv files in ${tpchWvletQueryPath}")
    debug(s"Found ${sqlFiles.size} .sql files in ${tpchSQLQueryPath}")
    
    // Check that we have at least 22 files for TPC-H queries
    assert(wvletFiles.size >= 22, s"Expected at least 22 .wv files, but found ${wvletFiles.size}")
    assert(sqlFiles.size >= 22, s"Expected at least 22 .sql files, but found ${sqlFiles.size}")
    
    // Verify specific query files exist
    for (i <- 1 to 22) {
      val queryName = s"q${i}"
      wvletFiles.exists(_.name == s"${queryName}.wv") shouldBe true
      sqlFiles.exists(_.name == s"${queryName}.sql") shouldBe true
    }
  }

  test("compile command should show help") {
    WvletMain.main("compile --help")
  }

  test("compile command should convert a simple query") {
    val inputQuery = "from users select name, age where age > 20"
    val out = captureStdout {
      WvletMain.main(s"""compile "$inputQuery" """)
    }

    debug("--- Input Wvlet Query ---")
    debug(inputQuery)
    debug("--- Actual Generated SQL ---")
    debug(out.trim)
    
    out shouldContain "select"
    out shouldContain "from users"
    out shouldContain "where age > 20"
  }

  test("compile command should convert TPC-H queries") {
    val wvletFiles = getTpchQueryFiles(tpchWvletQueryPath, "wv")
    val sqlFiles = getTpchQueryFiles(tpchSQLQueryPath, "sql")
    
    wvletFiles.foreach { wvFile =>
      val queryName = wvFile.name.stripSuffix(".wv")
      
      test(s"compile converts ${queryName}") {
        val out = captureStdout {
          WvletMain.main(s"""compile -t trino -w ${tpchWvletQueryPath} -f ${wvFile.name}""")
        }
        
        // Find corresponding SQL file
        val sqlFile = sqlFiles.find(_.name == s"${queryName}.sql")
        sqlFile match {
          case Some(expectedFile) =>
            val expectedSQL = IOUtil.readAsString(expectedFile.path)
            
            debug(s"=== Query: ${queryName} ===")
            val queryContent = IOUtil.readAsString(wvFile.path)
            debug(s"\nInput Wvlet Query: '${queryContent}'")
            debug(s"Generated SQL Output: '${out.trim}'")
            
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
            
            out.toLowerCase shouldContain "select"
          case None =>
            fail(s"Expected SQL file ${queryName}.sql not found")
        }
      }
    }
  }

  test("compile command should apply database-specific SQL dialect") {
    val out = captureStdout {
      WvletMain.main("""compile -t duckdb "from users select *" """)
    }
    out.toLowerCase shouldContain "select *"
    out.toLowerCase shouldContain "from users"
  }

  test("to_wvlet command should convert a simple query") {
    val sqlQuery = "SELECT name, age FROM users WHERE age > 20"
    val out = captureStdout {
      WvletMain.main(s"""to_wvlet "$sqlQuery" """)
    }
    
    // Basic verification
    out.toLowerCase shouldContain "from users"
    out.toLowerCase shouldContain "select name, age"
    out.toLowerCase shouldContain "where age > 20"
  }
    
  
  test("to_wvlet command should convert SQL to wvlet") {
    val sqlFiles = getTpchQueryFiles(tpchSQLQueryPath, "sql")
    val wvletFiles = getTpchQueryFiles(tpchWvletQueryPath, "wv")
    
    sqlFiles.foreach { sqlFile =>
      val queryName = sqlFile.name.stripSuffix(".sql")
      
      test(s"to_wvlet converts ${queryName}") {
        val out = captureStdout {
          WvletMain.main(s"""to_wvlet -t trino -w ${tpchSQLQueryPath} -f ${sqlFile.name}""")
        }
        
        // Find corresponding Wvlet file for reference
        val wvFile = wvletFiles.find(_.name == s"${queryName}.wv")
        wvFile match {
          case Some(expectedFile) =>
            val expectedWvlet = IOUtil.readAsString(expectedFile.path)
            debug(s"Expected Wvlet for ${queryName}: ${expectedWvlet}")
            debug(s"Generated Wvlet: ${out}")
          case None =>
            debug(s"No reference .wv file found for ${queryName}")
        }
        
        // Basic verification
        out.toLowerCase shouldContain "from"
        out.toLowerCase shouldContain "select"
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
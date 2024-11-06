package wvlet.lang.cli

import wvlet.airspec.AirSpec

import java.io.ByteArrayOutputStream

class WvletCompileTest extends AirSpec:
  test("help") {
    WvletMain.main("compile --help")
  }

  private def captureStdout(body: => Unit): String =
    val out = new ByteArrayOutputStream()
    Console.withOut(out) {
      body
    }
    val s = out.toString
    debug(s)
    s

  test("compile") {
    val out = captureStdout {
      WvletMain.main("""compile -w spec/basic "from 'person.json'" """)
    }
    out shouldContain "select *"
    out shouldContain "from 'spec/basic/person.json'"
  }

  test("compile save") {
    val out = captureStdout {
      WvletMain.main("""compile -w spec/basic "from 'person.json' save as 'tmp.parquet'" """)
    }
    out shouldContain "copy"
    out shouldContain "to 'spec/basic/tmp.parquet'"
    out shouldContain "select *"
    out shouldContain "from 'spec/basic/person.json'"
  }

end WvletCompileTest

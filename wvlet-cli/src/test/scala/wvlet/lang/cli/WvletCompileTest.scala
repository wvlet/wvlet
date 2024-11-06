package wvlet.lang.cli

import wvlet.airspec.AirSpec

import java.io.ByteArrayOutputStream
import scala.annotation.internal.Body

class WvletCompileTest extends AirSpec:
  test("help") {
    WvletMain.main("compile --help")
  }

  private def captureStdout(body: => Unit): String =
    val out = new ByteArrayOutputStream()
    Console.withOut(out) {
      body
    }
    out.toString

  test("compile") {
    val out = captureStdout {
      WvletMain.main("""compile -w spec/basic "from 'person.json'" """)
    }
    out shouldContain "select *"
    out shouldContain "from 'person.json'"
  }

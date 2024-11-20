package wvlet.lang.native

import wvlet.airspec.AirSpec

class WvcMainTest extends AirSpec:

  def captureOut(body: => Unit): String =
    val out = new java.io.ByteArrayOutputStream()
    Console.withOut(out) {
      body
    }
    debug(out)
    out.toString

  test("run command") {
    val out = captureOut {
      WvcMain.main(Array("-q", "select 1"))
    }
    out shouldContain "select 1"
  }

  test("use stdlib") {
    val out = captureOut {
      WvcMain.main(Array("-q", "select '1'.to_int"))
    }
    out shouldContain "cast('1' as bigint)"
  }

  test("load spec") {
    val out = captureOut {
      WvcMain.main(Array("-w", "spec/basic", "-q", "from person"))
    }
    out shouldContain "from 'spec/basic/person.json'"
  }

end WvcMainTest

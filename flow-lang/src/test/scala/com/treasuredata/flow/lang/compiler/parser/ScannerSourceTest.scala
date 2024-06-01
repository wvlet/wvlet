package com.treasuredata.flow.lang.compiler.parser

import wvlet.airspec.AirSpec

class ScannerSourceTest extends AirSpec:
  test("find line and column positions") {
    val src = ScannerSource.fromText("a\nb\nc")
    src.length shouldBe 5

    test("find line index") {
      src.offsetToLine(0) shouldBe 0
      src.offsetToLine(1) shouldBe 0
      src.offsetToLine(2) shouldBe 1
      src.offsetToLine(3) shouldBe 1
      src.offsetToLine(4) shouldBe 2
    }

    test("find column index") {
      src.offsetToColumn(0) shouldBe 1
      src.offsetToColumn(1) shouldBe 2
      src.offsetToColumn(2) shouldBe 1
      src.offsetToColumn(3) shouldBe 2
      src.offsetToColumn(4) shouldBe 1
    }

    test("find startOfLine") {
      src.startOfLine(0) shouldBe 0
      src.startOfLine(1) shouldBe 0
      src.startOfLine(2) shouldBe 2
      src.startOfLine(3) shouldBe 2
      src.startOfLine(4) shouldBe 4
    }
  }

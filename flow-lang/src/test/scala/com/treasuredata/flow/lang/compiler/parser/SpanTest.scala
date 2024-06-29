package com.treasuredata.flow.lang.compiler.parser

import wvlet.airspec.AirSpec

class SpanTest extends AirSpec:
  test("create a span") {
    val span = Span.within(1, 2)
    span.toString shouldBe "[1..2)"
    span.exists shouldBe true
    span.start shouldBe 1
    span.end shouldBe 2
    span.pointOffset shouldBe 0
    span.point shouldBe 1
  }

  test("create a point span") {
    val span = Span.at(1)
    span.toString shouldBe "[1..1)"
    span.exists shouldBe true
    span.start shouldBe 1
    span.end shouldBe 1
    span.pointOffset shouldBe 0
    span.point shouldBe 1
  }

  test("create a span with point") {
    val span = Span(1, 5, 2)
    span.toString shouldBe "[1..3..5)"
    span.exists shouldBe true
    span.start shouldBe 1
    span.end shouldBe 5
    span.pointOffset shouldBe 2
    span.point shouldBe 3
  }

  test("NoSpan") {
    val span = Span.NoSpan
    span.exists shouldBe false
    span.toString shouldBe "[NoSpan]"
  }

end SpanTest

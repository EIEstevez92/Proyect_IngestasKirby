package com.datio.kirby.input

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FixedReaderTest extends FunSuite with FixedReader {

  test("Return 2 when a ALPHANUMERIC(2) is received") {
    assert(size("ALPHANUMERIC(2)") === 2)
  }

  test("Return 5 when a ALPHANUMERIC(5) is received") {
    assert(size("ALPHANUMERIC(5)") === 5)
  }

  test("Return 256 when a ALPHANUMERIC(256) is received") {
    assert(size("ALPHANUMERIC(256)") === 256)
  }

  test("Return 512 when a ALPHANUMERIC(512) is received") {
    assert(size("ALPHANUMERIC(512)") === 512)
  }

  test("Return 512 when a ALPHANUMERIC(2)(512) is received") {
    assert(size("ALPHANUMERIC(2)(512)") === 512)
  }

  test("Return 4 when a DECIMAL(4) is received") {
    assert(size("DECIMAL(4)") === 4)
  }

  test("Return 4 when a DECIMAL(2) (4) is received") {
    assert(size("DECIMAL(2) (4)") === 4)
  }

  test("Return 14 when a DECIMAL(12,2) is received") {
    assert(size("DECIMAL(12,2)") === 14)
  }

  test("Return 12 when a DECIMAL(10,6) is received") {
    assert(size("DECIMAL(10,2)") === 12)
  }

  test("Return 45 when a DECIMAL(10,6)(45) is received") {
    assert(size("DECIMAL(10,2)(45)") === 45)
  }

  test("Return 9 when a NUMERIC LARGE is received") {
    assert(size("NUMERIC LARGE") === 9)
  }

  test("Return 10 when a NUMERIC LARGE(10) is received") {
    assert(size("NUMERIC LARGE(10)") === 10)
  }

  test("Return 4 when a NUMERIC SHORT is received") {
    assert(size("NUMERIC SHORT") === 4)
  }

  test("Return 4 when a NUMERIC SHORT(3) is received") {
    assert(size("NUMERIC SHORT  (3)") === 3)
  }

  test("Return 10 when a DATE is received") {
    assert(size("DATE") === 10)
  }

  test("Return 10 when a DATE(12) is received") {
    assert(size("DATE(12)") === 12)
  }

  test("Return 8 when a TIME is received") {
    assert(size("TIME") === 8)
  }

  test("Return 8 when a TIME(1) is received") {
    assert(size("TIME(1)") === 1)
  }

  test("Return 25 when a TIMESTAMP is received") {
    assert(size("TIMESTAMP") === 25)
  }

  test("Return 25 when a TIMESTAMP(26) is received") {
    assert(size("TIMESTAMP(26)") === 26)
  }

  test("Return 0 when an unknow value is received") {
    assert(size("XXASES") === 0)
  }
}

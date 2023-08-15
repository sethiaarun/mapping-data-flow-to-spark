package com.microsoft.azure.adf.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for StringUtil
 */
class StringUtilSpec extends AnyFlatSpec with should.Matchers {

  it should "add braces for the list" in {
    val input = List("1", "2", "3")
    assert(StringUtil.addBrackets(input) == "1(2(3))")
  }
  it should "add braces for 1 element" in {
    val input = List("1")
    assert(StringUtil.addBrackets(input) == "1")
  }
  it should "add braces for 2 elements" in {
    val input = List("1", "2")
    assert(StringUtil.addBrackets(input) == "1(2)")
  }
}

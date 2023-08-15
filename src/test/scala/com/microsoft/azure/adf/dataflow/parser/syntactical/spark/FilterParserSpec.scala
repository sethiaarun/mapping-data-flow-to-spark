package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for the filter parser
 */
class FilterParserSpec extends AnyFlatSpec with should.Matchers {
  val filterParser = new FilterParser()
  it should "parse the filter transformation input" in {
    val input = """dimcity filter(StateProvince=="California" || StateProvince=="Texas") ~> cityfilter"""
    assert(filterParser.parse(input).isDefined)
  }
  it should "parse the filter transformation input for multiple conditions" in {
    val input = "yellowtaxidataselect filter(total_amount>80 && total_distance<20) ~> yellowtaxipricefilter"
    assert(filterParser.parse(input).isDefined)
  }
  it should "fail the filter parsing when ( is missing after filter" in {
    val input = "yellowtaxidataselect filtertotal_amount>80) ~> yellowtaxipricefilter"
    assert(!filterParser.parse(input).isDefined)
  }
  it should "fail the filter parsing when ~> is missing after filter" in {
    val input = "yellowtaxidataselect filter(total_amount>80) yellowtaxipricefilter"
    assert(!filterParser.parse(input).isDefined)
  }
  it should "parse with functions used with column name" in {
    val input = "yellowtaxidataselect filter(ltrim(rtrim(lower(wfci_triggername)))== ltrim(rtrim(lower(TriggerName)))) ~> test1"
    assert(filterParser.parse(input).isDefined)
  }
}

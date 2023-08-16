package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for the sort parser
 */
class SortParserSpec extends AnyFlatSpec with should.Matchers {
  val sortParser = new SortParser()
  it should "parse the sort transformation" in {
    val input =
      """
        |cityselect sort(desc(City, true),
        |	desc(State, false),
        |	caseInsensitive:true,
        |	partitionLevel:true) ~> sort1
        |""".stripMargin
    assert(sortParser.parse(input).isDefined)
  }
  it should "parse sort transformation no properties are defined" in {
    val input =
      """
        |cityselect sort(asc(City, true), desc(State, false)) ~> sort1
        |""".stripMargin
    assert(sortParser.parse(input).isDefined)
  }
}

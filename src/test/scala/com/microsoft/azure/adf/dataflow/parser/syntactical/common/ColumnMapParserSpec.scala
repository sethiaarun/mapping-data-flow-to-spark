package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParserTest
import com.microsoft.azure.adf.dataflow.semanticmodel.SparkCodeGenerator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ColumnMapParserSpec extends AnyFlatSpec with should.Matchers {

  // dummy class implementation for test cases
  class DummyColumnMapParser extends BaseStandardTokenParserTest
    with ColumnMapParser {
    /**
     * root parser for given keyword/function
     * This is used by parse function, ideally a parser should have one and only one parser
     * as public visibility
     *
     * @return
     */
    override def root_parser: Parser[SparkCodeGenerator] = ???

    // for test cases
    override def multiMapCol_rule: Parser[Map[String, String]] = super.multiMapCol_rule

    // for test cases
    override def colMap_rule = super.colMap_rule
  }

  val colMapParser = new DummyColumnMapParser()

  it should "parse column map with destination=source" in {
    val input = "destinationCol=sourceCol"
    assert(colMapParser.parse(input, colMapParser.colMap_rule).isDefined)
  }
  it should "parse column map with destination=source.subCol" in {
    val input = "destinationCol=sourceCol.subCol"
    assert(colMapParser.parse(input, colMapParser.colMap_rule).isDefined)
  }
  it should "parse column map with source column only without destination column name" in {
    val input = "sourceCol"
    assert(colMapParser.parse(input, colMapParser.colMap_rule).isDefined)
  }
  it should "parse multi column map" in {
    val input =
      """
        |s1=d1,
        |s2=d2,
        |d3,
        |d4,
        |s5=d5
        |""".stripMargin
    assert(colMapParser.parse(input, colMapParser.multiMapCol_rule).isDefined)
  }
}

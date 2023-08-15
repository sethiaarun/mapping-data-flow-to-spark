package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for the parameter parser
 */
class ParameterParserSpec extends AnyFlatSpec with should.Matchers {
  val paramParser = new ParameterParser()
  it should "parse the parameters" in {
    val input ="""parameters{
                 |	FileName as string ("abs.parquet"),
                 |	TestName as string ("Developmental Test"),
                 |	paramHs as integer (0)
                 |}""".stripMargin
    assert(paramParser.parse(input).isDefined)
  }
}

package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.semanticmodel.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.ExpressionConditionParser
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.matching.Regex

/**
 * test cases for [[ExpressionConditionParser]]
 */
class ExpressionConditionParsersSpec extends AnyFlatSpec with should.Matchers with PrivateMethodTester {

  class DummyExpressionConditionParser extends BaseStandardTokenParserTest
    with ExpressionConditionParser {
    /**
     * root parser for given keyword/function
     * This is used by parse function, ideally a parser should have one and only one parser
     * as public visibility
     *
     * @return
     */
    override def root_parser: Parser[SparkCodeGenerator] = ???

  }

  val dummyExCodParser = new DummyExpressionConditionParser()

  it should "parse simple expression" in {
    val input = "x==2"
    assert(dummyExCodParser.parse(input, dummyExCodParser.expressCondition_rule).isDefined)
  }
  it should "parse expression with logical and/or" in {
    val input = "x==2 && y==4 || c>5"
    assert(dummyExCodParser.parse(input, dummyExCodParser.expressCondition_rule).isDefined)
  }
  it should "parse expression with logical and/or with functions" in {
    val input = "fn(fn1(fn2(x)))==2 && fn5(fn6(y))>=4"
    assert(dummyExCodParser.parse(input, dummyExCodParser.expressCondition_rule).isDefined)
  }
  it should "throw exception when un-identified logical operator" in {
    val input = "fn(fn1(fn2(x)))+=5"
    assert(!dummyExCodParser.parse(input, dummyExCodParser.expressCondition_rule).isDefined)
  }
}

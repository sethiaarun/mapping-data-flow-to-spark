package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.model.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.ExpressionConditionParser
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.matching.Regex

/**
 * test cases for [[ExpressionConditionParser]]
 */
class ExpressionConditionParsersSpec extends AnyFlatSpec with should.Matchers with PrivateMethodTester {

  class DummyExpressionConditionParser extends BaseStandardTokenParser with ExpressionConditionParser {
    /**
     * unimplemented function - name of parser
     * This is useful to cache list of parsers in Map with keyname as name
     * So it should be unique for each parser
     *
     * @return
     */
    override def name(): String = "testExpressionConditionParser"

    /**
     * description of parser - friendly descrition detail
     *
     * @return
     */
    override def description(): String = "testExpressionConditionParser"

    /**
     * root parser for given keyword/function
     * This is used by parse function, ideally a parser should have one and only one parser
     * as public visibility
     *
     * @return
     */
    override def root_parser: Parser[SparkCodeGenerator] = ???

    /**
     * This function defines matching regex rule when this parser should be applied
     *
     * @return
     */
    override def matchRegExRule: Regex = ???

    /**
     * parse input using given parser
     *
     * @param input
     * @param parser
     * @tparam T
     * @return
     */
    def parse[T](input: String, parser: Parser[T]): Option[T] = {
      val tokens = new lexical.Scanner(input)
      phrase(parser)(tokens) match {
        case Success(tree, _) => Option(tree)
        case ex: NoSuccess =>
          None
      }
    }
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

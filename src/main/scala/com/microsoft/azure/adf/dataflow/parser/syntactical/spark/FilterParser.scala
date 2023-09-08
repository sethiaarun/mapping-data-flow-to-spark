package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.semanticmodel.filter.DataFlowFilter
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.{ExpressionConditionParser, KeyValueColonSeparatedParser}

import scala.util.matching.Regex

/**
 * Filter Parser to parse
 * inputvariable filter(total_amount>45 && trip_distance>1) ~> outputvariable
 * or inputvariable filter(ltrim(rtrim(field1)=='Test' && trip_distance>1) ~> outputvariable
 * The current implementation supports functions with one argument and no additional parameters.
 * That means It will fail for round(field,2)
 */
class FilterParser extends BaseStandardTokenParser
  with ExpressionConditionParser
  with KeyValueColonSeparatedParser {

  /**
   * parse entire filter element
   * inputvariable filter(filter expressions*) ~> outputvariable
   *
   * @return
   */
  override def root_parser: Parser[DataFlowFilter] =
    (inputVar_rule ~ expressCondition_rule ~ outputVarName_rule) ^^ {
      case inputVar ~ filterExp ~ outputVar =>
        DataFlowFilter(inputVar, filterExp, outputVar)
    }

  /**
   * input source for the filter
   * Example - inputvar filter(
   *
   * @return
   */
  private def inputVar_rule: Parser[String] = (ident <~ "filter") ^^ { case inputVar => inputVar }

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bfilter\\(".r
  }

  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "FilterTransformationParser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse Filters"

}

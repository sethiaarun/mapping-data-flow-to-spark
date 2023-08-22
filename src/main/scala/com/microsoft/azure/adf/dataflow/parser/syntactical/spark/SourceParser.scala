package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.parser.syntactical.common.{ColumnDefinitionParser, KeyValueColonSeparatedParser}
import com.microsoft.azure.adf.dataflow.semanticmodel.column.ColumnDefinitionList
import com.microsoft.azure.adf.dataflow.semanticmodel.source.{DataFlowSource, DataFlowSourceOutPut}

import scala.util.matching.Regex

/**
 * Source Output Parser
 *
 * source(output(
 * VendorID as long)
 * ),
 * format: 'parquet') ~> outputvariable
 */
class SourceParser extends BaseStandardTokenParser
  with KeyValueColonSeparatedParser
  with ColumnDefinitionParser {

  /**
   * source related standard token parsing ends
   *
   * @return
   */
  def root_parser: Parser[DataFlowSource] =
    ("source" ~ "(" ~> outputExpr_rule ~ "," ~ multiKvColonSep_rule ~ ")" ~ outputVarName_rule) ^^ {
      case output ~ "," ~ source_properties ~ ")" ~ vn => DataFlowSource(output, source_properties, vn)
    }

  /**
   * output with list of name and their type
   *
   * @return
   */
  private def outputExpr_rule: Parser[DataFlowSourceOutPut] = ("output" ~ "(" ~> colDefinition_rule <~ ")") ^^ {
    case l => DataFlowSourceOutPut(ColumnDefinitionList(l))
  }

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bsource\\(output\\(".r
  }

  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "SourceOutPut"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse Source and Output"

  // source related standard token parsing ends
}

package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.semanticmodel.sink.DataFlowSink
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.KeyValueColonSeparatedParser

import scala.util.matching.Regex

/**
 * Sink Parser
 *
 * input variable sink(
 * key:value,
 * format: 'parquet') ~> outputvariable
 */
class SinkParser extends BaseStandardTokenParser
  with KeyValueColonSeparatedParser {

  def root_parser: Parser[DataFlowSink] =
    (ident ~ "sink" ~ "(" ~ multiKvColonSep_rule ~ ")" ~ outputVarName_rule) ^^ {
      case inputVarName ~ "sink" ~ "(" ~ sinkProperties ~ ")" ~ vr =>
        DataFlowSink(inputVarName, sinkProperties, vr)
    }

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bsink\\(".r
  }

  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "SinkTransformationParser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse Sink"

}

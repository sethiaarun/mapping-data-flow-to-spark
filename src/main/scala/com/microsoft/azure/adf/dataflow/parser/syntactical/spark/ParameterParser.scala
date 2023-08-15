package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.model.param.{ListParameter, Parameter}
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.ColumnDefinitionParser

import scala.util.matching.Regex

/**
 * parse parameters from the script code
 * parameters{
 * FileName as string ("Test"),
 * FlowName as string ("Test1"),
 * count as integer (0)
 * }
 * It supports String and Integer, but can be extended for other data types from [[ListParameter]]
 */
class ParameterParser extends BaseStandardTokenParser
  with ColumnDefinitionParser {


  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "parameterparser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "parser to parse parameters from the script code"

  /**
   * root parser for given keyword/function
   *
   * @return
   */
  override def root_parser: Parser[ListParameter] = ("parameters" ~ "{" ~ list_param_rule ~ "}") ^^ {
    case "parameters" ~ "{" ~ listParam ~ "}" => ListParameter(listParam)
  }

  /**
   * list of parameter extraction rule
   *
   * @return
   */
  private def list_param_rule: Parser[List[Parameter]] = repsep(param_rule, ",")

  /**
   * parameter extraction rule
   *
   * @return
   */
  private def param_rule: Parser[Parameter] = (columnDefinition_rule ~ "(" ~ (stringLit | numericLit) ~ ")") ^^ {
    case nameType ~ "(" ~ pValue ~ ")" => Parameter(nameType.name, nameType.f_type, pValue)
  }

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = "\\bparameters\\b".r
}

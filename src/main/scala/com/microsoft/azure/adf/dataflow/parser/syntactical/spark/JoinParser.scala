package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.model.join.{JoinFlow, JoinRule}
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.{CommonUsableParser, ExpressionConditionParser, KeyValueColonSeparatedParser}

import scala.util.matching.Regex

/**
 *
 * Join Parser to parse
 * <<left stream>>, <<right stream>> join(
 *     left stream field <<condition>> right stream field,
 *     joinType:<<left/right/inner>>,
 *     matchType:'exact',
 *     ignoreSpaces:false,
 *     etc...
 * ) ~> <<output variable>>
 *   The join conditions can be complex with logical conditional operator and functions with single argument.
 *   That means It will fail for round(field,2)
 */
class JoinParser extends BaseStandardTokenParser
  with ExpressionConditionParser
  with CommonUsableParser
  with KeyValueColonSeparatedParser {

  // private data type for internal uses
  case class LeftRightJoinVar(leftVar: String, rightVar: String)

  /**
   * overall join parser
   *
   * @return
   */
  override def root_parser: Parser[JoinFlow] = (leftJoinWithRight_rule ~ join_rule ~ outputVarName_rule)^^ {
    case leftRightJoinVar ~ join_rule ~ vn =>JoinFlow(leftRightJoinVar.leftVar, leftRightJoinVar.rightVar, join_rule,vn)}

  /**
   * get left and right variable
   * <<left stream>>, <<right stream>>
   * @return
   */
  private def leftJoinWithRight_rule: Parser[LeftRightJoinVar] = (ident ~ "," ~ ident) ^^ {
    case leftStreamVar ~ "," ~ rightStreamVar => LeftRightJoinVar(leftStreamVar, rightStreamVar)
  }

  /**
   * join rule
   * join(
   *     left stream field <<condition>> right stream field,
   *     joinType:<<left/right/inner>>,
   *     matchType:'exact',
   *     ignoreSpaces:false,
   *     etc...
   * )
   * @return
   */
  private def join_rule: Parser[JoinRule] = ("join" ~> expressCondition_rule ~ "," ~ multiKvColonSep_rule ~ ")") ^^ {
    case filterExp ~ "," ~ kvProp ~ ")" =>
      JoinRule(filterExp, kvProp)
  }

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bjoin\\(".r
  }

  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "JoinParser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse join relationship"

}

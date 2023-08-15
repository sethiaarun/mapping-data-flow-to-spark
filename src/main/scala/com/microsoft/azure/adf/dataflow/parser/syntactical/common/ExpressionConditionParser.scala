package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.model.filter.{ExpressionCondition, ListExpressionCondition}
import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

/**
 * Expression condition parser - used for filter, join, etc.
 */
trait ExpressionConditionParser extends LogicalOperatorParser
  with OperandParser {
  this: BaseStandardTokenParser =>

  // this will parse filter condition where left operand ~ logical operator ~ right operand and logical condition
  // It is a root parser from this trait
  def expressCondition_rule: Parser[ListExpressionCondition] = chainExpCond_rule ^^ { case list => ListExpressionCondition(list) }

  /**
   * chain list of expression conditions with logical condition operator
   * like a > b && c<d
   *
   * @return
   */
  private def chainExpCond_rule: Parser[List[ExpressionCondition]] =
    rep((filterCondition_rule ~ logicalConditionOperator_rule) ^^ {
      case fex ~ lop => fex.copy(logicalCondition = lop)
    } | filterCondition_rule ^^ { case fex => fex })

  /**
   * This will parse filter condition where left operand ~ logical operator ~ right operand
   * like a > b or a < c, etc.
   *
   * @return
   */
  private def filterCondition_rule: Parser[ExpressionCondition] = (operand ~ logicalOperator_rule ~ operand) ^^ {
    case operandL ~ logicalOperator ~ operandR => ExpressionCondition(operandL, logicalOperator.opName, operandR)
  }

  /**
   * return list of "identifiers" or functions + identifier, if return size >1 then last element is field
   * and remaining are functions to applied on that field
   * It does not validate number of open and close brackets, it assumes the script code has right syntax
   *
   * @return
   */
  private def operand: Parser[List[String]] = (chainBracketIdentifier_rule <~ rep(bracketClosing_rule)) ^^ {
    case fnFields => fnFields
  }

  /**
   * closing bracket
   *
   * @return
   */
  private def bracketClosing_rule: Parser[String] = (")") ^^^ {
    ")"
  }

  /**
   * chain bracketIdentifier_rule with "(" if present then return a
   *
   * @return
   */
  private def chainBracketIdentifier_rule: Parser[List[String]] = chainl1(bracketIdentifier_rule, "(" ^^^ {
    (left: List[String], right: List[String]) => left ::: right
  })

  /**
   * within expression function names or field/variable are wrapped around the field name
   * in such scenario it will parse them
   *
   * @return
   */
  private def bracketIdentifier_rule: Parser[List[String]] = rep("(" ~>
    columnOperand_rule | stringLit | ident | numericLit | parameterOperand_rule
  )
}

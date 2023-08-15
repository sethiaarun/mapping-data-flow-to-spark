package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.model.LogicalOperator
import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

/**
 * List of logical Operator Parser
 */
trait LogicalOperatorParser {
  this: BaseStandardTokenParser =>

  /**
   * list of logical operator
   *
   * @return
   */
  protected def logicalOperator_rule: Parser[LogicalOperator] = ("==" | "<=" | ">=" | "<" | ">" | "===" | "!=") ^^ {
    case op => LogicalOperator(op)
  }

  /**
   * lis of logical condition operator
   *
   * @return
   */
  protected def logicalConditionOperator_rule: Parser[String] = ("&&" | "||") ^^ {
    case op => op
  }
}

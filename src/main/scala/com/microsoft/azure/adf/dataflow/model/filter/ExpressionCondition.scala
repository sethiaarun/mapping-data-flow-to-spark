package com.microsoft.azure.adf.dataflow.model.filter

/**
 * It is a Filter expression Model - Does not support spark code generation by itself
 *
 * @param operandL         Left list of operand
 * @param operator         operator
 * @param operandR         Right Operand
 * @param logicalCondition like &&, ||, etc.
 */
case class ExpressionCondition(operandL: List[String], operator: String,
                               operandR: List[String], logicalCondition: String = "")


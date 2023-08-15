package com.microsoft.azure.adf.dataflow.constant

/**
 * list of constant operator
 */
object Operator {

  type Op = String
  val EQ: Op = "=="
  val EQ_NULL: Op = "==="
  val LT_EQ: Op = "<="
  val GT_EQ: Op = ">="
  val LT: Op = "<"
  val GT: Op = ">"
  val NOT_EQ: Op = "!="
  val ASSIGN: Op = "="
}

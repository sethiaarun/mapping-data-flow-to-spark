package com.microsoft.azure.adf.dataflow.model.filter

/**
 * It is a Filter expression Model - Does not support spark code generation by itself
 * TODO: add logic for OperandR, so far we are assuming only left hand side we will have field
 *
 * @param operandL         Left list of operand
 * @param operator         operator
 * @param operandR         Right Operand
 * @param logicalCondition like &&, ||, etc.
 */
case class ExpressionCondition(operandL: List[String], operator: String,
                               operandR: List[String], logicalCondition: String = "") {

  /**
   * get Scala Left Operand
   *
   * @return
   */
  def scalaOperandL(): List[String] =
    _operandBuilder(operandL, (source: String, field: String) => s"""${source}("${field}")""")

  /**
   * get Scala Right Operand
   *
   * @return
   */
  def scalaOperandR(): List[String] =
    _operandBuilder(operandR, (source: String, field: String) => s"""${source}("${field}")""")


  /**
   * pySpark Left Operand
   *
   * @return
   */
  def pySparkL(): List[String] =
    _operandBuilder(operandL, (source: String, field: String) => s"""${source}.${field}""")

  /**
   * pySpark Right Operand
   *
   * @return
   */
  def pySparkR(): List[String] =
    _operandBuilder(operandR, (source: String, field: String) => s"""${source}.${field}""")

  /**
   *
   * @param list
   * @return
   */
  private def _operandBuilder(list: List[String], fn: (String, String) => String): List[String] = list.map(op => {
    if (op.contains("@")) {
      val sourceField: Array[String] = op.split("@")
      fn(sourceField(0), sourceField(1))
    } else {
      op
    }
  })

}


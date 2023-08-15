package com.microsoft.azure.adf.dataflow.model.filter

import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}
import com.microsoft.azure.adf.util.StringUtil

/**
 * This model class provides filter operation detail for the spark code generation
 *
 * @param inputVarName             input variable name
 * @param lstExpression          list of filter expressions
 * @param output_assigned_variable output variable name
 */
case class DataFlowFilter(inputVarName: String, lstExpression: ListExpressionCondition, output_assigned_variable: String)
  extends Expr with SparkCodeGenerator {

  private val DATAFLOW_LOGICAL_COND_MAP = Map("==" -> "===", "<=" -> "<=", ">=" -> ">=", "<" -> "<=", ">" -> ">", "===" -> "===", "!=" -> "!=")

  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${inputVarName}.filter(${getListFilters()})"""
  }

  /**
   * PYSpark Code
   *
   * @return
   */
  override def pySparkCode(): String = {
    s"""${output_assigned_variable}=${inputVarName}.filter(${getListFilters()})"""
  }

  /**
   * get list of filters from filter expression
   * Assumption - left operand will be field name and right will be a value
   * It includes logical conditional operator as well.
   * TODO: we need to fix multiple issues here, for example left operand can be value and right can be field
   * or both side left and right can be field from dataframe, etc.
   */
  private def getListFilters(): String = {
    lstExpression.list.map(filter => {
      val leftOperand = StringUtil.addBrackets(addInputSourceToColumn(filter.operandL))
      val rightOperand = StringUtil.addBrackets(filter.operandR)
      val p = if (!filter.logicalCondition.isEmpty) {
        s"${leftOperand} ${DATAFLOW_LOGICAL_COND_MAP.getOrElse(filter.operator, "===")} ${rightOperand} ${filter.logicalCondition}"
      } else {
        s"${leftOperand} ${DATAFLOW_LOGICAL_COND_MAP.getOrElse(filter.operator, "===")} ${rightOperand}"
      }
      p
    }).mkString(" ")
  }

  /**
   * add source or input dataframe name with the last field in the list
   * for example as list may have ltrim, rtrim, trim and then field name
   * we would like to have ltrim, rtrim, trim, inputdataframe(fieldname)
   *
   * @param list
   * @return
   */
  private def addInputSourceToColumn(list: List[String]): List[String] = {
    val lastElem = list.reverse.head
    list.init :+ s"""${inputVarName}("${lastElem}")"""
  }
}
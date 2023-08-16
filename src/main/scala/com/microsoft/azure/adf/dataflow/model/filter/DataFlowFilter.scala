package com.microsoft.azure.adf.dataflow.model.filter

import com.microsoft.azure.adf.dataflow.`type`.LanguageType.{Language, PYTHON, SCALA}
import com.microsoft.azure.adf.dataflow.constant.LogicalConditionMapping.{DATAFLOW_LOGICAL_COND_MAP_PY, DATAFLOW_LOGICAL_COND_MAP_SCALA}
import com.microsoft.azure.adf.dataflow.constant.LogicalOperatorMapping.{DATAFLOW_LOGICAL_OPERATOR_MAP_PY, DATAFLOW_LOGICAL_OPERATOR_MAP_SCALA}
import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}
import com.microsoft.azure.adf.util.StringUtil

/**
 * This model class provides filter operation detail for the spark code generation
 *
 * @param inputVarName             input variable name
 * @param lstExpression            list of filter expressions
 * @param output_assigned_variable output variable name
 */
case class DataFlowFilter(inputVarName: String, lstExpression: ListExpressionCondition, output_assigned_variable: String)
  extends Expr with SparkCodeGenerator {

  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${inputVarName}.filter(${getScalaListFilters()})"""
  }

  /**
   * PYSpark Code
   *
   * @return
   */
  override def pySparkCode(): String = {
    s"""${output_assigned_variable}=${inputVarName}.filter(${getPySparkListFilters()})"""
  }

  /**
   * get list of filters from filter expression
   * Assumption - left operand will be field name and right will be a value
   * It includes logical conditional operator as well.
   * TODO: we need to fix multiple issues here, for example left operand can be value and right can be field
   * or both side left and right can be field from dataframe, etc.
   */
  private def getScalaListFilters(): String = {
    lstExpression.list.map(filter => {
      val leftOperand = StringUtil.addBrackets(addSparkInputSourceToColumn(filter.scalaOperandL, SCALA))
      val rightOperand = StringUtil.addBrackets(filter.scalaOperandR)
      val p = if (!filter.logicalCondition.isEmpty) {
        val scalaLogicalCondition = DATAFLOW_LOGICAL_COND_MAP_SCALA.getOrElse(filter.logicalCondition, "||")
        s"${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_SCALA.getOrElse(filter.operator, "===")} ${rightOperand} ${scalaLogicalCondition}"
      } else {
        s"${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_SCALA.getOrElse(filter.operator, "===")} ${rightOperand}"
      }
      p
    }).mkString(" ")
  }

  /**
   * get list of filters from filter expression
   * Assumption - left operand will be field name and right will be a value
   * It includes logical conditional operator as well.
   * TODO: we need to fix multiple issues here, for example left operand can be value and right can be field
   * or both side left and right can be field from dataframe, etc.
   */
  private def getPySparkListFilters(): String = {
    lstExpression.list.map(filter => {
      val leftOperand = StringUtil.addBrackets(addSparkInputSourceToColumn(filter.pySparkL, PYTHON))
      val rightOperand = StringUtil.addBrackets(filter.pySparkR)
      val p = if (!filter.logicalCondition.isEmpty) {
        val pySparkLogicalCondition = DATAFLOW_LOGICAL_COND_MAP_PY.getOrElse(filter.logicalCondition, "|")
        s"(${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_PY.getOrElse(filter.operator, "===")} ${rightOperand}) ${pySparkLogicalCondition}"
      } else {
        s"(${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_PY.getOrElse(filter.operator, "===")} ${rightOperand})"
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
  private def addSparkInputSourceToColumn(listOfOperand: List[String], language: Language): List[String] = {
    val lastElem = listOfOperand.reverse.head
    language match {
      case SCALA =>
        listOfOperand.init :+ s"""${inputVarName}("${lastElem}")"""
      case PYTHON =>
        listOfOperand.init :+ s"""${inputVarName}.${lastElem}"""
    }
  }
}
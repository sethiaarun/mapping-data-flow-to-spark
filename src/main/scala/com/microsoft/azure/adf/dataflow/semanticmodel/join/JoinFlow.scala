package com.microsoft.azure.adf.dataflow.semanticmodel.join

import com.microsoft.azure.adf.dataflow.`type`.LanguageType.{Language, PYTHON, SCALA}
import com.microsoft.azure.adf.dataflow.constant.LogicalConditionMapping.{DATAFLOW_LOGICAL_COND_MAP_PY, DATAFLOW_LOGICAL_COND_MAP_SCALA}
import com.microsoft.azure.adf.dataflow.constant.LogicalOperatorMapping.{DATAFLOW_LOGICAL_OPERATOR_MAP_PY, DATAFLOW_LOGICAL_OPERATOR_MAP_SCALA}
import com.microsoft.azure.adf.dataflow.semanticmodel.{Expr, SparkCodeGenerator}
import com.microsoft.azure.adf.util.StringUtil

/**
 * This semantic model class provides Join operation detail for the spark code generation
 *
 * @param leftStream               left data source
 * @param rightStream              right data source
 * @param joinRule                 join rules like field and
 * @param output_assigned_variable output variable name
 */
case class JoinFlow(leftStream: String, rightStream: String, joinRule: JoinRule, output_assigned_variable: String)
  extends Expr
    with SparkCodeGenerator {

  override def scalaSparkCode(): String = {
    val joinCondition = s"${getScalaListOfJoinConditions()},${getJoinType(SCALA)}"
    s"""val ${output_assigned_variable}=${leftStream}.join(${rightStream},${joinCondition})"""
  }

  override def pySparkCode(): String = {
    val joinCondition = s"${getPySparkListOfJoinConditions()},${getJoinType(PYTHON)}"
    s"""${output_assigned_variable}=${leftStream}.join(${rightStream},${joinCondition})"""
  }


  /**
   * get list of join expression with logical condition for Scala Spark
   *
   */
  private def getScalaListOfJoinConditions(): String = {
    joinRule.lstExpCondition.list.map(filter => {
      val leftOperand = StringUtil.addBrackets(filter.scalaOperandL)
      val rightOperand = StringUtil.addBrackets(filter.scalaOperandR)
      val p = if (!filter.logicalCondition.isEmpty) {
        val scalaLogicalCondition = DATAFLOW_LOGICAL_COND_MAP_SCALA.getOrElse(filter.logicalCondition, "||")
        s"${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_SCALA.getOrElse(filter.operator, filter.operator)} ${rightOperand} ${scalaLogicalCondition}"
      } else {
        s"${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_SCALA.getOrElse(filter.operator, filter.operator)} ${rightOperand}"
      }
      p
    }).mkString(" ")
  }

  /**
   * get list of join expression with logical condition for PySpark
   *
   */
  private def getPySparkListOfJoinConditions(): String = {
    joinRule.lstExpCondition.list.map(filter => {
      val leftOperand = StringUtil.addBrackets(filter.pySparkL)
      val rightOperand = StringUtil.addBrackets(filter.pySparkR)
      val p = if (!filter.logicalCondition.isEmpty) {
        val scalaLogicalCondition = DATAFLOW_LOGICAL_COND_MAP_PY.getOrElse(filter.logicalCondition, "|")
        s"${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_PY.getOrElse(filter.operator, filter.operator)} ${rightOperand} ${scalaLogicalCondition}"
      } else {
        s"${leftOperand} ${DATAFLOW_LOGICAL_OPERATOR_MAP_PY.getOrElse(filter.operator, filter.operator)} ${rightOperand}"
      }
      p
    }).mkString(" ")
  }

  /**
   * get join type
   *
   * @param language
   * @return
   */
  private def getJoinType(language: Language) = {
    s""""${joinRule.joinProp.getOrElse("joinType", "inner", language)}"""".stripMargin
  }

}

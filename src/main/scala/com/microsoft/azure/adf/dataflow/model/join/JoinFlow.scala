package com.microsoft.azure.adf.dataflow.model.join

import com.microsoft.azure.adf.dataflow.`type`.LanguageType.{Language, PYTHON, SCALA}
import com.microsoft.azure.adf.dataflow.constant.Operator._
import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}
import com.microsoft.azure.adf.util.StringUtil

/**
 * This model class provides Join operation detail for the spark code generation
 *
 * @param leftStream               left data source
 * @param rightStream              right data source
 * @param joinRule                 join rules like field and
 * @param output_assigned_variable output variable name
 */
case class JoinFlow(leftStream: String, rightStream: String, joinRule: JoinRule, output_assigned_variable: String)
  extends Expr
    with SparkCodeGenerator {

  private val JOIN_MAPPING = Map(EQ -> EQ_NULL, LT_EQ -> LT_EQ, GT_EQ -> GT_EQ,
    LT_EQ -> LT_EQ, GT -> GT, EQ_NULL -> EQ_NULL, NOT_EQ -> NOT_EQ)

  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${leftStream}.join(${rightStream},${getJoinCondition(SCALA)})"""
  }

  override def pySparkCode(): String = {
    s"""${output_assigned_variable}=${leftStream}.join(${getJoinCondition(PYTHON)})"""
  }


  /**
   * get list of join expression with logical condition
   *
   */
  private def getListOfJoinConditions(): String = {
    joinRule.lstExpCondition.list.map(filter => {
      val leftOperand = StringUtil.addBrackets(filter.operandL)
      val rightOperand = StringUtil.addBrackets(filter.operandR)
      val p = if (!filter.logicalCondition.isEmpty) {
        s"${leftOperand} ${JOIN_MAPPING.getOrElse(filter.operator, filter.operator)} ${rightOperand} ${filter.logicalCondition}"
      } else {
        s"${leftOperand} ${JOIN_MAPPING.getOrElse(filter.operator, filter.operator)} ${rightOperand}"
      }
      p
    }).mkString(" ")
  }

  /**
   * get join condition
   *
   * @param language
   * @return
   */
  private def getJoinCondition(language: Language) = {
    s"""${getListOfJoinConditions()},"${joinRule.joinProp.getOrElse("joinType", "inner", language)}"""".stripMargin
  }

}

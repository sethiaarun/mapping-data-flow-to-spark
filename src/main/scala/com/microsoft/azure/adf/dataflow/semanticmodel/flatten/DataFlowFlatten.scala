package com.microsoft.azure.adf.dataflow.semanticmodel.flatten

import com.microsoft.azure.adf.dataflow.semanticmodel.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.semanticmodel.{Expr, SparkCodeGenerator}

/**
 * This semantic model class provides dataflow flatten transformation
 *
 * @param inputVarName
 * @param columnMap
 * @param unrollFields  data to be flatten from array
 * @param isMultiUnroll is it multi unroll
 * @param colMapProperties
 * @param output_assigned_variable
 */
case class DataFlowFlatten(inputVarName: String, unrollFields: List[String], isMultiUnroll: Boolean,
                           columnMap: Map[String, String], colMapProperties: ListKeyValueProperties,
                           output_assigned_variable: String)
  extends Expr with SparkCodeGenerator {
  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${inputVarName}.${getUnrollBy()}.select(${getListOfColWithAlias()})""".stripMargin
  }

  /**
   * PYSpark Code
   *
   * @return
   */
  override def pySparkCode(): String = {
    s""" ${output_assigned_variable}=${inputVarName}.${getUnrollBy()}.select(${getListOfColWithAlias()})""".stripMargin
  }


  /**
   * if it is multi unroll then unroll root will not be there all
   * fields are considered for unroll
   *
   * @return
   */
  private def getUnrollBy(): String = if (isMultiUnroll) {
    unrollFields.map(field => s"""withColumn("${field}", explode(col("${field}")))""").mkString(".")
  } else {
    s"""withColumn("${unrollFields.head}", explode(col("${unrollFields.head}")))"""
  }

  /**
   * get list of columns with alias
   *
   * @return
   */
  private def getListOfColWithAlias(): String = {
    columnMap.map { case (d, s) => {
      s"""col("${s}").alias("${d}")"""
    }
    }.mkString(",")
  }

  /**
   * get Unroll root, if multi unroll then no root
   * TODO: need to add logic for Unroll root
   *
   * @return
   */
  private def getUnrollRoot: Option[String] = if (!isMultiUnroll && unrollFields.size > 1) Some(unrollFields(1)) else None
}

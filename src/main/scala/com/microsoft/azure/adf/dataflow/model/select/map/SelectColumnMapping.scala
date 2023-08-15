package com.microsoft.azure.adf.dataflow.model.select.map

import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}

/**
 * This model class provides select column mapping operation detail for the spark code generation
 *
 * @param inputVarName
 * @param columnMap
 * @param colMapProperties
 * @param output_assigned_variable
 */
case class SelectColumnMapping(inputVarName: String, columnMap: Map[String, String],
                               colMapProperties: ListKeyValueProperties, output_assigned_variable: String)
  extends Expr with SparkCodeGenerator {
  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${inputVarName}.select(${getListOfColWithAlias()})"""
  }

  /**
   * PYSpark Code
   *
   * @return
   */
  override def pySparkCode(): String = {
    s"""${output_assigned_variable}=${inputVarName}.select(${getListOfColWithAlias()})"""
  }

  private def getListOfColWithAlias() = {
    columnMap.map { case (k, v) => {
      s"""col("${v}").alias("${k}")"""
    }
    }.mkString(",")
  }
}

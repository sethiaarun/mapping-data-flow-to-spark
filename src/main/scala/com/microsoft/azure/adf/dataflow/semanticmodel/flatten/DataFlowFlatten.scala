package com.microsoft.azure.adf.dataflow.semanticmodel.flatten

import com.microsoft.azure.adf.dataflow.semanticmodel.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.semanticmodel.{Expr, SparkCodeGenerator}

/**
 * This semantic model class provides dataflow flatten transformation
 *
 * @param inputVarName
 * @param columnMap
 * @param unrollCOl
 * @param colMapProperties
 * @param output_assigned_variable
 */
case class DataFlowFlatten(inputVarName: String, unrollCOl: List[String], columnMap: Map[String, String],
                           colMapProperties: ListKeyValueProperties, output_assigned_variable: String)
  extends Expr with SparkCodeGenerator {
  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    ""
  }

  /**
   * PYSpark Code
   *
   * @return
   */
  override def pySparkCode(): String = {
    ""
  }

}

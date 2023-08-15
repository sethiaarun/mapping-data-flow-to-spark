package com.microsoft.azure.adf.dataflow.model.sort

import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}
import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties

/**
 * Data flow Sort transformation
 *
 * @param inputVarName
 * @param lstSortField
 * @param sortProperties
 * @param output_assigned_variable
 */
case class DataFlowSort(inputVarName: String, lstSortField: List[SortField],
                        sortProperties: ListKeyValueProperties, output_assigned_variable: String) extends Expr
                        with SparkCodeGenerator {
  override def scalaSparkCode(): String = ???

  override def pySparkCode(): String = ???
}

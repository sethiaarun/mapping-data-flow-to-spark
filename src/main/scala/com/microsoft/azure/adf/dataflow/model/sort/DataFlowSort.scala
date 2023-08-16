package com.microsoft.azure.adf.dataflow.model.sort

import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}

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
  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${inputVarName}.select("*").orderBy(${getSortColumnOperation()})"""
  }

  override def pySparkCode(): String = {
    s"""${output_assigned_variable}=${inputVarName}.select("*").orderBy(${getSortColumnOperation()})"""
  }


  private def getSortColumnOperation(): String = {
    lstSortField.map(sf => {
      val sortDirection: String = sf.nullFirst match {
        case true =>
          // at the end open and closing brackets are require for PySpark
          // for scala spark it is optional
          s"""${sf.sortDirection}_nulls_first()"""
        case _ => s"""${sf.sortDirection}()"""
      }
      s"""col("${sf.fieldName}").${sortDirection}"""
    }).mkString(",")
  }
}

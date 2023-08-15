package com.microsoft.azure.adf.dataflow.model.sort

import com.microsoft.azure.adf.dataflow.model.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.model.sort.SortType.SortType

/**
 *
 * @param sortDirection
 * @param fieldName
 * @param nullFirst
 */
case class SortField(sortDirection: SortType, fieldName: String,nullFirst:Boolean) extends SparkCodeGenerator {
  override def scalaSparkCode(): String = ???

  override def pySparkCode(): String = ???
}

package com.microsoft.azure.adf.dataflow.model.union

import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}

/**
 * This model class provides union operation detail for the spark code generation
 * TODO: add by position later
 *
 * @param inSource
 * @param unionWith
 * @param propMap for example byName:true, etc.
 * @param output_assigned_variable
 */
case class UnionMapping(inSource: String, unionWith: String,
                        propMap: ListKeyValueProperties, output_assigned_variable: String)
  extends Expr with SparkCodeGenerator {
  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${inSource}.unionByName(${unionWith})"""
  }

  /**
   * PYSpark Code
   *
   * @return
   */
  override def pySparkCode(): String = {
    s"""${output_assigned_variable}=${inSource}.select(${unionWith})"""
  }
}

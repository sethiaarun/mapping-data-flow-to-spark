package com.microsoft.azure.adf.dataflow.model

/**
 * spark code generator
 */
trait SparkCodeGenerator {
  def scalaSparkCode(): String

  def pySparkCode(): String
}

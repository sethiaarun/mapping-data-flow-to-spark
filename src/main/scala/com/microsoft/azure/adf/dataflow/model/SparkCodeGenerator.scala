package com.microsoft.azure.adf.dataflow.model

/**
 * spark code generator
 * It is used along with syntactical parsers to generate spark code
 */
trait SparkCodeGenerator {

  /**
   * Get Scala Spark Code
   *
   * @return
   */
  def scalaSparkCode(): String

  /**
   * get PySparkCode
   *
   * @return
   */
  def pySparkCode(): String

}

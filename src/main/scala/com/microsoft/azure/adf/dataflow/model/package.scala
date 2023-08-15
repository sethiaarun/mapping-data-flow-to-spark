package com.microsoft.azure.adf.dataflow

package object model {

  implicit class SparkCodeBuilder(listSparkCode: List[SparkCodeGenerator]) {

    /**
     * from given list of SparkCodeGenerator, generate scala spark code
     *
     * @param listSparkCode
     * @return
     */
    def scalaSparkCode(): List[String] = {
      listSparkCode.map(_.scalaSparkCode())
    }

    /**
     * from given list of SparkCodeGenerator, generate pySpark code
     *
     * @param listSparkCode
     * @return
     */
    def pySparkCode(): List[String] = {
      listSparkCode.map(_.pySparkCode())
    }

  }
}

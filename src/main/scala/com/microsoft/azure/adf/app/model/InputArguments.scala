package com.microsoft.azure.adf.app.model

import com.microsoft.azure.adf.dataflow.source.SourceTypes.SourceType

/**
 * Application Input arguments
 */
trait InputArguments extends Product {

  val sourceType: SourceType

  //class name used for spark scala class
  val className: String
  //spark job application name
  val appName: String

  // fabric runtime environment detail
  val fabricRunTimeEnv: FabricRunTimeEnv

  lazy val mapValue: Map[SourceType, Any] = {
    this.getClass.getDeclaredFields.map(_.getName).zip(this.productIterator).toMap
  }
}

package com.microsoft.azure.adf.dataflow

import com.microsoft.azure.adf.dataflow.writer.formatter.CodeFormatter
import com.microsoft.azure.adf.dataflow.writer.template.SparkCode
import com.typesafe.scalalogging.Logger

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Writer implicit classes and functions
 */
package object writer {
  val logger = Logger(getClass.getName)


  implicit class WriteSparkCode(sparkCode: SparkCode) {
    def write[T <: CodeFormatter](fileExtension: String,
                                       writerArgs: Map[String, Any])(implicit ct: ClassTag[T], tg: TypeTag[T]): String = {
      val fileFormatter = FileFormatCodeWriter
        .apply(fileExtension)
        .applyCode(sparkCode)
        .usingArguments(writerArgs)
        .build()
      val filePath = fileFormatter.write[T]()
      logger.info(s"Notebook file generated ${filePath}")
      filePath
    }
  }
}

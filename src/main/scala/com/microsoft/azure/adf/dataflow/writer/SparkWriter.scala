package com.microsoft.azure.adf.dataflow.writer

import com.microsoft.azure.adf.dataflow.writer.formatter.CodeFormatter

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
/**
 * Spark code writer
 */
trait SparkWriter {

  /**
   * Write using code formatter
   * @param ct
   * @param tg
   * @tparam T
   * @return
   */
  def write[T <: CodeFormatter]()(implicit ct: ClassTag[T], tg: TypeTag[T]): String
}

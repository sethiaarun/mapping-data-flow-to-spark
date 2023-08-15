package com.microsoft.azure.adf.dataflow.model.sink

import com.microsoft.azure.adf.dataflow.`type`.LanguageType.{Language, PYTHON, SCALA}
import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}

/**
 * This model class provides Sink operation detail for the spark code generation
 *
 * @param inputVarName             input variable name
 * @param sinkProperties           sink properties
 * @param output_assigned_variable output variable
 */
case class DataFlowSink(inputVarName: String, sinkProperties: ListKeyValueProperties,
                        output_assigned_variable: String) extends Expr with SparkCodeGenerator {
  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    s"""val ${output_assigned_variable}=${inputVarName}.write.format("${getDataSourceFormat(SCALA)}").save(${getSinkFilePath(SCALA)})"""
  }


  override def pySparkCode(): String = {
    f"""${output_assigned_variable}=${inputVarName}.write.save(${getSinkFilePath(PYTHON)},format="${getDataSourceFormat(PYTHON)}")"""
  }

  /**
   * get data source from source options
   *
   * @return
   */
  def getDataSourceFormat(lang: Language): String = sinkProperties.getOrElse("format", "parquet", lang)

  /**
   * get source file path
   *
   * @param lang
   * @return
   */
  def getSinkFilePath(lang: Language): String = {
    val fileSystem = sinkProperties.getOrElse("fileSystem", "<container>", lang)
    val folderPath = sinkProperties.getOrElse("folderPath", "<folder path>", lang)
    val abfsPath = s"abfss://${fileSystem}@<storage-account-name>.dfs.core.windows.net/${folderPath}"
    lang match {
      case SCALA =>
        s"""s"${abfsPath}""""
      case _ =>
        s"""f"${abfsPath}""""
    }
  }
}

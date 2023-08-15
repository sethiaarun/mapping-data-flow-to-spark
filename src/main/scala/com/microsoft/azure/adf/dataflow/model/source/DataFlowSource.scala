package com.microsoft.azure.adf.dataflow.model.source

import com.microsoft.azure.adf.dataflow.`type`.LanguageType.{Language, PYTHON, SCALA}
import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.model.{Expr, SparkCodeGenerator}
import com.microsoft.azure.adf.util.StringUtil

/**
 * This model class provides Source operation detail for the spark code generation
 *
 * @param output                   output
 * @param sourceProperties         list of options configured for the source
 * @param output_assigned_variable output assigned variable
 */
case class DataFlowSource(output: DataFlowSourceOutPut, sourceProperties: ListKeyValueProperties, output_assigned_variable: String)
  extends Expr with SparkCodeGenerator {

  /**
   * Scala Spark Code
   *
   * @return
   */
  override def scalaSparkCode(): String = {
    List(s"""val ${output_assigned_variable}=spark.read.format("${getDataSourceFormat(SCALA)}").load(${getSourceFilePath(SCALA)})""",
      s"""select(${output.nameTypeList.list.map(f => s"\"${f.name}\"").mkString(",")})""")
      .mkString(".")
  }

  /**
   * PYSpark Code
   *
   * @return
   */
  override def pySparkCode(): String = {
    List(s"""${output_assigned_variable}=spark.read.load(${getSourceFilePath(PYTHON)},format="${getDataSourceFormat(PYTHON)}")""",
      s"""select(${output.nameTypeList.list.map(f => s"\"${f.name}\"").mkString(",")})""")
      .mkString(".")
  }

  /**
   * get data source from source options
   *
   * @return
   */
  def getDataSourceFormat(language: Language): String = sourceProperties.getOrElse("format", "parquet", language)

  /**
   * get source file path
   *
   * @param language
   * @return
   */
  def getSourceFilePath(language: Language): String = {
    val fileSystem: String = sourceProperties.getOrElse("fileSystem", "<container>", language)
    val folderPath: Option[String] = sourceProperties.get("folderPath", language)
    val fileName: Option[String] = sourceProperties.get("fileName", language)
    val path: String = StringUtil.concatString("/", folderPath, fileName)
    val abfsPath = s"abfss://${fileSystem}@<storage-account-name>.dfs.core.windows.net/${path}"
    language match {
      case SCALA =>
        s"""s"${abfsPath}""""
      case _ =>
        s"""f"${abfsPath}""""
    }
  }

}

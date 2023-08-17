package com.microsoft.azure.adf.tool

import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate
import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate.TemplatePath
import com.microsoft.azure.adf.dataflow.model.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.parser.text.MappingDataFlowParser
import com.microsoft.azure.adf.dataflow.writer._
import com.microsoft.azure.adf.dataflow.writer.formatter.{CodeFormatter, PyCodeFormatter, ScalaFileCodeFormatter}
import com.microsoft.azure.adf.dataflow.writer.template.{SparkCode, SparkFileTemplateReader}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Spark Code Generator
 * It will parse list of line codes read from script code file
 * and translate them in individual step a step is separated by ~> output variable
 * later apply different parser to parse them into spark code
 */
class CodeGenerator(dataFlowScriptCode: List[String], templateArgs: Map[String, Any])
  extends MappingDataFlowParser {


  private val parsedSparkCode: List[SparkCodeGenerator] = parse(dataFlowScriptCode)

  /**
   * Generate Scala Spark Object
   * It will use [[ScalaTemplateReader]] to create the scala spark code and save it using
   */
  def generateScalaSparkCode(): String = {
    //get scala spark code , apply that to the template
    // and save them using file writer
    // the file name will be picked from templateArgs
    _generateFileSparkCode[ScalaFileCodeFormatter](DataFlowCodeTemplate.SCALASPARK_TEMLATE,
      ".scala",
      parsedSparkCode.scalaSparkCode()
    )
  }

  /**
   * Generate PySpark Code
   * It will use [[ScalaTemplateReader]] to create the scala spark code and save it using
   *
   */
  def generatePySparkCode(): String = {
    _generateFileSparkCode[PyCodeFormatter](DataFlowCodeTemplate.PYSPARKCODE_TEMPLATE,
      ".py",
      parsedSparkCode.pySparkCode())
  }


  /**
   * generate Spark code in File (Class/Object/Script)
   *
   * @param codeTemplatePath
   * @param fileExtension
   * @param listOfCodeLines
   * @param ct
   * @param tg
   * @tparam T
   * @return
   */
  private def _generateFileSparkCode[T <: CodeFormatter](codeTemplatePath: TemplatePath,
                                                         fileExtension: String,
                                                         listOfCodeLines: List[String])
                                                        (implicit ct: ClassTag[T], tg: TypeTag[T]): String = {
    SparkFileTemplateReader
      .readTemplate(codeTemplatePath)
      .applyArguments(templateArgs)
      .withCode(listOfCodeLines)
      .build()
      .getCode()
      .write[T](fileExtension, templateArgs)
  }


  /**
   * generate Scala Spark NoteBook
   *
   * @param metaDataPath notebook metadata
   * @param suffix       notebook file suffix
   * @return
   */
  def generateScalaNoteBook(metaDataPath: TemplatePath, suffix: String): String = {
    val scalaImportStmt = SparkCode("import org.apache.spark.sql.functions._")
    val listCodeStatements = scalaImportStmt :: parsedSparkCode.map(code => SparkCode(code.scalaSparkCode()))
    _generateSparkNoteBook[ScalaFileCodeFormatter](metaDataPath, suffix, listCodeStatements)
  }

  /**
   * generate PySpark Notebook
   *
   * @param metaDataPath notebook metadata
   * @param suffix       notebook file suffix
   * @return
   */
  def generatePySparkNoteBook(metaDataPath: TemplatePath, suffix: String): String = {
    val pyNbImportStmt = SparkCode("from pyspark.sql.functions import *")
    val listCodeStatements = pyNbImportStmt :: parsedSparkCode.map(code => SparkCode(code.pySparkCode()))
    _generateSparkNoteBook[PyCodeFormatter](metaDataPath, suffix, listCodeStatements)
  }

  /**
   * generate PySpark Notebook
   *
   * @param metaDataPath notebook metadata
   * @param suffix       notebook file suffix
   * @return
   */
  private def _generateSparkNoteBook[T <: CodeFormatter](metaDataPath: TemplatePath,
                                                         suffix: String,
                                                         listSparkCode: List[SparkCode])
                                                        (implicit ct: ClassTag[T], tg: TypeTag[T]): String = {
    NbFormatWriter
      .readMetaData(metaDataPath)
      .applyArguments(templateArgs)
      .withCode(listSparkCode)
      .fileSuffix(suffix)
      .build()
      .write[T]()
  }

}

package com.microsoft.azure.adf.tool

import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate
import com.microsoft.azure.adf.dataflow.model.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.parser.text.MappingDataFlowParser
import com.microsoft.azure.adf.dataflow.writer._
import com.microsoft.azure.adf.dataflow.writer.formatter.{PyCodeFormatter, ScalaFileCodeFormatter}
import com.microsoft.azure.adf.dataflow.writer.template.{SparkCode, SparkFileTemplateReader}


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
    SparkFileTemplateReader
      .apply(DataFlowCodeTemplate.SCALASPARK_TEMLATE)
      .usingArguments(templateArgs)
      .applyCode(parsedSparkCode.scalaSparkCode())
      .build()
      .getCode()
      .write[ScalaFileCodeFormatter](".scala", templateArgs)
  }

  /**
   * Generate PySpark Code
   * It will use [[ScalaTemplateReader]] to create the scala spark code and save it using
   *
   */
  def generatePySparkCode(): String = {
    SparkFileTemplateReader
      .apply(DataFlowCodeTemplate.PYSPARKCODE_TEMPLATE)
      .usingArguments(templateArgs)
      .applyCode(parsedSparkCode.pySparkCode())
      .build()
      .getCode()
      .write[PyCodeFormatter](".py", templateArgs)
  }


  /**
   * generate Scala Spark NoteBook
   *
   * @return
   */
  def generateScalaNoteBook(): String = {
    NbFormatWriter
      .apply(DataFlowCodeTemplate.SCALASPARK_NOTEBOOK_METADATA)
      .usingArguments(templateArgs)
      .applyCode(parsedSparkCode.map(code => SparkCode(code.scalaSparkCode())))
      .withFileSuffix("spark")
      .build()
      .write[ScalaFileCodeFormatter]()
  }

  /**
   * generate PySpark NoteBook
   *
   * @return
   */
  def generatePySparkNoteBook(): String = {
    NbFormatWriter
      .apply(DataFlowCodeTemplate.PYSPARK_NOTEBOOK_METADATA)
      .usingArguments(templateArgs)
      .applyCode(parsedSparkCode.map(code => SparkCode(code.pySparkCode())))
      .withFileSuffix("pyspark")
      .build()
      .write[PyCodeFormatter]()
  }

}

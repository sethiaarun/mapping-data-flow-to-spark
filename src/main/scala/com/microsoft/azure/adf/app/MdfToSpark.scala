package com.microsoft.azure.adf.app

import com.microsoft.azure.adf.app.model.InputArguments
import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate.{PYSPARK_FABRIC_NOTEBOOK_METADATA, SCALASPARK_FABRIC_NOTEBOOK_METADATA}
import com.microsoft.azure.adf.dataflow.source.MdfCodeSourceRegistry
import com.typesafe.scalalogging.Logger

/**
 * Entry program to convert MDF to Spark
 */
object MdfToSpark extends ApplicationArgumentParser with MdfCodeSourceRegistry {

  val logger = Logger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // parse input arguments
    val inputArgument: InputArguments = parseAppArg(args)

    getSourceType(inputArgument.sourceType) match {
      case Some(sourceObj) =>
        sourceObj.scriptCodeLines(inputArgument.asInstanceOf[sourceObj.E]) match {
          case Some(mdfSource) =>
            val codeGenerator: CodeGenerator = new CodeGenerator(mdfSource.listOfCodeLines, inputArgument)
            codeGenerator.generateScalaSparkCode()
            codeGenerator.generatePySparkCode()
            codeGenerator.generatePySparkNoteBook(PYSPARK_FABRIC_NOTEBOOK_METADATA, "py_fabric")
            codeGenerator.generateScalaNoteBook(SCALASPARK_FABRIC_NOTEBOOK_METADATA, "scala_fabric")
          case _ =>
            logger.error("not able to find any script code lines")
        }
      case _ =>
        logger.error("no matching source type found")
    }
  }
}

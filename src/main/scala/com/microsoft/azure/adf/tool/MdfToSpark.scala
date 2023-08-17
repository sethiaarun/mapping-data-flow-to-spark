package com.microsoft.azure.adf.tool

import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate._
import com.microsoft.azure.adf.mdf.source.MdfCodeSourceRegistry
import com.microsoft.azure.adf.util.ApplicationArgumentParser
import com.typesafe.scalalogging.Logger

/**
 * Entry program to convert MDF to Spark
 */
object MdfToSpark extends ApplicationArgumentParser with MdfCodeSourceRegistry {

  val logger = Logger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // parse input arguments
    val cliArgs: Map[String, String] = parseAppArg(args)
    cliArgs.get("source") match {
      case Some(sourceType) =>
        // File or API - MdfCodeSourceRegistry
        getSourceType(sourceType) match {
          case Some(sourceObj) =>
            sourceObj.scriptCodeLines(cliArgs) match {
              case Some(mdfSource) =>
                val codeGenerator: CodeGenerator = new CodeGenerator(mdfSource.listOfCodeLines, mdfSource.parsedArgs)
                codeGenerator.generateScalaSparkCode()
                codeGenerator.generatePySparkCode()
                codeGenerator.generatePySparkNoteBook(PYSPARK_FABRIC_NOTEBOOK_METADATA,"py_fabric")
                codeGenerator.generateScalaNoteBook(SCALASPARK_FABRIC_NOTEBOOK_METADATA,"scala_fabric")
                codeGenerator.generatePySparkNoteBook(PYSPARK_SYNAPSE_NOTEBOOK_METADATA,"py_synapse")
                codeGenerator.generateScalaNoteBook(SCALASPARK_SYNAPSE_NOTEBOOK_METADATA,"scala_synapse")
              case _ =>
                logger.error("not able to find any script code lines")
            }
          case _ =>
            logger.error("no matching source type found")
        }
      case _ =>
        logger.error("Source type is missing")
    }
  }
}

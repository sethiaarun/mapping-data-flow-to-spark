package com.microsoft.azure.adf.tool

import com.microsoft.azure.adf.mdf.source.MdfCodeSourceRegistry
import com.microsoft.azure.adf.util.ApplicationArgumentParser
import com.typesafe.scalalogging.Logger

import scala.collection.mutable
import scala.io.Source
import scala.sys.exit

/**
 * Entry program to convert MDF to Spark
 */
object MdfToSpark extends ApplicationArgumentParser with MdfCodeSourceRegistry {

  val logger = Logger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // parse input arguments
    val cliArgs: Map[String, String] = parseAppArg(args)
    cliArgs.get("source") match {
      case Some(st) =>
        getSourceType(st.toString) match {
          case Some(sourceObj) =>
            sourceObj.scriptCodeLines(cliArgs) match {
              case Some(mdfSource) =>
                val codeGenerator: CodeGenerator = new CodeGenerator(mdfSource.listOfCodeLines, mdfSource.parsedArgs)
                codeGenerator.generateScalaSparkCode()
                codeGenerator.generatePySparkCode()
                codeGenerator.generatePySparkNoteBook()
                codeGenerator.generateScalaNoteBook()
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

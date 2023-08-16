package com.microsoft.azure.adf.dataflow.writer

import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate.MetaDataPath
import com.microsoft.azure.adf.dataflow.writer.formatter.CodeFormatter
import com.microsoft.azure.adf.dataflow.writer.template.SparkCode
import com.typesafe.scalalogging.Logger
import jep.SharedInterpreter

import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Notebook code writer
 *
 * @param fileSuffix
 * @param metadataPath
 * @param sparkCode
 * @param fileArgs
 */
protected class NbFormatWriter(val fileSuffix: String,
                               val metadataPath: MetaDataPath,
                               val sparkCode: List[SparkCode],
                               val fileArgs: Map[String, Any])
  extends SparkWriter {

  private val logger = Logger(getClass.getName)

  /**
   * auxiliary constructor
   *
   * @param metadataPath
   */
  protected def this(metadataPath: MetaDataPath) = this("", metadataPath, Nil, Map.empty)

  /**
   * output file extension
   *
   * @return
   */
  def fileExtension: String = ".ipynb"


  /**
   * file suffix
   *
   * @param fileArgs
   * @return
   */
  def fileSuffix(fileSuffix: String): NbFormatWriter =
    new NbFormatWriter(fileSuffix, this.metadataPath, this.sparkCode, this.fileArgs)


  /**
   * using arguments
   *
   * @param fileArgs
   * @return
   */
  def applyArguments(fileArgs: Map[String, Any]): NbFormatWriter =
    new NbFormatWriter(this.fileSuffix, this.metadataPath, this.sparkCode, fileArgs)

  /**
   * apply the following code
   *
   * @param sparkCode
   * @return
   */
  def withCode(sparkCode: List[SparkCode]): NbFormatWriter =
    new NbFormatWriter(this.fileSuffix, this.metadataPath, sparkCode, this.fileArgs)

  /**
   * Define default notebook
   *
   * @param name
   * @return
   */
  private def noteBookMetaData(): String = {
    val stream = Source.fromResource(metadataPath)
    val input: List[String] = stream.getLines().toList
    val text = input.mkString("\n")
    stream.close()
    text
  }

  def build(): NbFormatWriter =
    new NbFormatWriter(this.fileSuffix, this.metadataPath, this.sparkCode, this.fileArgs)

  /**
   * Write nbformat notebook for Spark code
   *
   * @param listOfSparkCode
   * @param fileArgs
   * @return
   */
  def write[T <: CodeFormatter]()(implicit ct: ClassTag[T], tg: TypeTag[T]): String = {
    require(this.sparkCode.length > 0, "Spark code is missing")
    require(this.fileArgs.size > 0, "File arguments are missing")
    require(!this.metadataPath.isBlank, "No metadata file path found")
    val fileName = fileArgs.getOrElse("className", "TestFlow").toString + fileSuffix
    val interp = new SharedInterpreter
    // import require python libraries
    interp.exec("import nbformat")
    interp.exec("from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell")

    // create a new notebook with metadata
    interp.exec(s"nb = new_notebook(metadata=${noteBookMetaData.split("\n").mkString(" ")})")

    // add list of cells with scala spark code
    sparkCode.zipWithIndex.map { case (spCode, index) => {
      val code = s"""${spCode.code.replace("\n", "\\r")}"""
      interp.exec(s"""cell${index}=new_code_cell('${code}')""")
      interp.exec(s"nb['cells'].append(cell${index})")
    }
    }
    //write notebook
    val fileWrite =
      s"""
         |with open('${fileName}${fileExtension}', 'w', encoding='utf-8') as f:
         |   nbformat.write(nb, f)
         |""".stripMargin
    interp.exec(fileWrite)
    interp.close()
    logger.info(s"Notebook file generated ${fileName}")
    fileName
  }
}

/**
 * Companion object for NbFormatWriter class
 */
object NbFormatWriter {
  /**
   * template path
   *
   * @param metadataPath
   * @return
   */
  def readMetaData(metadataPath: MetaDataPath) = new NbFormatWriter(metadataPath)
}

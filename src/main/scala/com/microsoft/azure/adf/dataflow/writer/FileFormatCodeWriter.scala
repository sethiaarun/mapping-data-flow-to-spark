package com.microsoft.azure.adf.dataflow.writer

import com.microsoft.azure.adf.dataflow.writer.formatter.CodeFormatter
import com.microsoft.azure.adf.dataflow.writer.template.SparkCode

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * write code to a file and format using formatter
 *
 * @param fileExtension   File extension
 * @param listOfSparkCode list of spark code lines
 * @param fileArgs        file arguments like name of class, etc.
 * @param codeFormatter   code formatter to format the code
 */
protected class FileFormatCodeWriter(val fileExtension: String,
                                     val listOfSparkCode: List[SparkCode],
                                     val fileArgs: Map[String, Any])
  extends SparkWriter {
  /**
   * @param fileExtension
   */
  protected def this(fileExtension: String) =
    this(fileExtension, Nil, Map.empty)

  /**
   * using file arguments
   *
   * @param fileArgs
   * @return
   */
  def usingArguments(fileArgs: Map[String, Any]): FileFormatCodeWriter =
    new FileFormatCodeWriter(this.fileExtension, this.listOfSparkCode, fileArgs)

  /**
   * apply the following code
   *
   * @param listOfSparkCode
   * @return
   */
  def applyCode(listOfSparkCode: List[SparkCode]): FileFormatCodeWriter = {
    new FileFormatCodeWriter(this.fileExtension, listOfSparkCode, this.fileArgs)
  }


  /**
   * apply the following code
   *
   * @param sparkCode
   * @return
   */
  def applyCode(sparkCode: SparkCode): FileFormatCodeWriter = {
    new FileFormatCodeWriter(this.fileExtension, List(sparkCode), this.fileArgs)
  }

  /**
   * object builder
   *
   * @return
   */
  def build() =
    new FileFormatCodeWriter(this.fileExtension, this.listOfSparkCode, this.fileArgs)

  /**
   * write spark code output
   *
   * @param sparkCode
   */
  def write[T <: CodeFormatter]()(implicit ct: ClassTag[T], tg: TypeTag[T]): String = {
    require(!this.fileExtension.isEmpty, "Missing file extension")
    require(!this.listOfSparkCode.isEmpty, "Code is missing")
    require(!this.fileArgs.isEmpty, "File arguments are missing")
    val fileName = fileArgs.getOrElse("className", "TestFlow") + fileExtension
    val sparkCode = listOfSparkCode.map(spCode => spCode.code).mkString("\n")
    // format code if formatter is defined
    val codeFormatter: CodeFormatter = ct.runtimeClass.getDeclaredConstructor().newInstance().asInstanceOf[CodeFormatter]
    val formattedCode: String = codeFormatter.format(sparkCode, Paths.get(fileName)) match {
      case code if (!code.isEmpty) => code
      case code if (code.isEmpty) => sparkCode
    }
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(formattedCode)
    bw.close()
    file.getAbsolutePath
  }
}

/**
 * Companion object for FileFormatCodeWriter class
 */
object FileFormatCodeWriter {
  def apply(fileExtension: String): FileFormatCodeWriter = {
    new FileFormatCodeWriter(fileExtension)
  }
}
package com.microsoft.azure.adf.dataflow.writer.formatter

import com.microsoft.azure.adf.util.FileUtil

import java.net.URI
import java.nio.file.Path
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

/**
 * Scala Spark File Code Formatter
 */
class ScalaFileCodeFormatter extends CodeFormatter {

  private val formatterConf = "formatter/conf/.scalafmt.conf"

  /**
   * format the given code
   *
   * @param code              code to be formatted
   * @param outputFileAbsPath output file absolute path
   * @return
   */
  override def format(code: String, outputFileAbsPath: Path): String = {
    import org.scalafmt.interfaces.Scalafmt
    val classloader = Thread.currentThread.getContextClassLoader
    val scalafmt = Scalafmt.create(classloader)
    // jdk.nio.zipfs.ZipPath is not working with Scalafmt
    // hack to create a temporary file with same content and use it for the formatting
    val stream: BufferedSource = Source.fromResource(formatterConf)
    val formatConfLines: List[String] = stream.getLines().toList
    val tempFormatterConf: URI =
      FileUtil.createTmpFile(".scalafmt", ".conf", formatConfLines, "\n")
    scalafmt.format(Path.of(tempFormatterConf), outputFileAbsPath, code)
  }
}


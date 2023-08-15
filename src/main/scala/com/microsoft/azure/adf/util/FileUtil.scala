package com.microsoft.azure.adf.util

import java.io.{File, PrintWriter}
import java.net.URI

/**
 * File utility functions
 */
object FileUtil {

  /**
   * create temporary file from given content; where each list item is separated by given separator
   * It returns the Temp File URI
   *
   * @param prefix
   * @param suffix
   * @param content
   * @param separator
   * @return
   */
  def createTmpFile(prefix: String, suffix: String, content: List[String], separator: String = ""): URI = {
    val tempFile = File.createTempFile(prefix, suffix)
    val writer = new PrintWriter(tempFile)
    content.foreach(line => writer.write(line + separator))
    writer.close()
    tempFile.deleteOnExit()
    tempFile.toURI
  }
}

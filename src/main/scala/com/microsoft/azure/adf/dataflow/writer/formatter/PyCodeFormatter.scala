package com.microsoft.azure.adf.dataflow.writer.formatter

import java.nio.file.Path

/**
 * PySpark Code Formatter
 */
class PyCodeFormatter extends CodeFormatter {

  /**
   * format the given code
   *
   * @param code              code to be formatted
   * @param outputFileAbsPath output file absolute path
   * @return
   */
  def format(code: String, outputFileAbsPath: Path): String = {
    //TODO: add python code formatter to format the input code
    code
  }
}

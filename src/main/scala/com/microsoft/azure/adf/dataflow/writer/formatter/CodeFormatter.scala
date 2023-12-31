package com.microsoft.azure.adf.dataflow.writer.formatter

import java.nio.file.Path

/**
 * Code formatter, to format code generated by this tool
 */
trait CodeFormatter {

  /**
   * format the given code
   */
  def format(code: String, outputFileAbsPath: Path): String
}

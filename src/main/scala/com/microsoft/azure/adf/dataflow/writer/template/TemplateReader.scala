package com.microsoft.azure.adf.dataflow.writer.template

import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate.TemplatePath

import scala.io.Source

/**
 * Template template for various template needs for the code generation
 */
trait TemplateReader {

  val TEMPLATE_CODE_PLACE_HOLDER = "code"

  /**
   * template path for the template template
   *
   * @return
   */
  def TEMPLATE_PATH: TemplatePath

  /**
   * read template from resources/etc.
   *
   * @return
   */
  def readTemplate(): String = {
    val stream = Source.fromResource(TEMPLATE_PATH)
    val input: List[String] = stream.getLines().toList
    val text = input.mkString("\n")
    stream.close()
    text
  }


  /**
   * build template template object
   *
   * @return
   */
  def build(): TemplateReader


  /**
   * get spark code after applying the template
   *
   * @return
   */
  def getCode(): SparkCode

}

package com.microsoft.azure.adf.dataflow.writer.template

import com.microsoft.azure.adf.dataflow.constant.DataFlowCodeTemplate.TemplatePath
import org.apache.commons.text.StringSubstitutor

import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * scala spark template template to generate spark scala code
 *
 * @param templatePath
 * @param code         line of codes
 * @param templateArgs arguments for the template
 */
protected class SparkFileTemplateReader(val templatePath: TemplatePath,
                                        val code: List[String],
                                        val templateArgs: Map[String, Any])
  extends TemplateReader {

  /**
   *
   * @param templatePath
   */
  protected def this(templatePath: TemplatePath) = this(templatePath, Nil, Map.empty)

  /**
   * using arguments
   *
   * @param templateArgs
   * @return
   */
  def usingArguments(templateArgs: Map[String, Any]): SparkFileTemplateReader =
    new SparkFileTemplateReader(this.templatePath, this.code, templateArgs)

  /**
   * apply the following code
   *
   * @param code
   * @return
   */
  def applyCode(code: List[String]): SparkFileTemplateReader = {
    val args = templateArgs + ("code" -> code.mkString("\n"))
    new SparkFileTemplateReader(this.templatePath, code, args)
  }

  /**
   * read a template and apply arguments with the template
   *
   * @return
   */
  override def getCode(): SparkCode = {
    require(!this.templateArgs.isEmpty, "File arguments are missing")
    require(!this.templatePath.isEmpty, "Template path is missing")
    require(!this.code.isEmpty, "writing code is missing")
    SparkCode(StringSubstitutor.replace(readTemplate, templateArgs.asJava))
  }

  /**
   * template path for the template template
   *
   * @return
   */
  override def TEMPLATE_PATH: String = this.templatePath

  /**
   * build template template object
   *
   * @return
   */
  override def build(): TemplateReader = new SparkFileTemplateReader(this.templatePath, this.code, this.templateArgs)
}

/**
 * Companion object for ScalaSparkTemplateReader class
 */
object SparkFileTemplateReader {
  /**
   * template path
   *
   * @param templatePath
   * @return
   */
  def apply(templatePath: TemplatePath) = new SparkFileTemplateReader(templatePath)
}
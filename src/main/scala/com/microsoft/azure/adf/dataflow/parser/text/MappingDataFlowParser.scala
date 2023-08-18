package com.microsoft.azure.adf.dataflow.parser.text

import com.microsoft.azure.adf.dataflow.model.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.model.param.{ListParameter, Parameter}
import com.microsoft.azure.adf.dataflow.parser.ParserRegistry
import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.ParameterParser
import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec


/**
 * This is going to parse the Script code generated from Mapping Data Flow
 * and generates Scala Spark, PySpark and Notebook code
 */
trait MappingDataFlowParser extends ParserRegistry {

  private val logger = Logger(getClass.getName)

  /**
   * parse given input using [[MappingDataFlowScriptParser]]
   * The flow is as following:
   * 1. Parse first line and see if parameters are defined
   * 2. if parameters are defined then extract them from the main input and parse them using Parameter Parser
   * 3. Parse remaining lines of code into individual steps to that we can apply appropriate parser
   * 4. while passing the result first pass parameters and then sequential remaining steps code
   *
   * @param input
   * @return
   */
  def parse(input: List[String]): List[SparkCodeGenerator] = {
    if (!input.isEmpty) {
      // if first line starts with parameters{ that means parameters are available
      val (parameterLines, remainingLines) = if (input.head.startsWith("parameters{")) {
        findParameterLines(input, Nil, false)
      } else {
        ("", input)
      }
      // get list of parameters in map format key as name and value as Parameter object
      val paramMap: Map[String, Parameter] = parseParameterLines(parameterLines)
      // for remaining lines get individual steps
      val steps = scriptCodeSteps(remainingLines, Nil, Nil)
      val codeList: List[SparkCodeGenerator] = steps.flatMap(step => {
        // for each step get parser
        val keyName = findParserName(step)
        keyName match {
          case Some(key) =>
            val parserObject = getParserObject(key).get
            Some(parserObject.parse(step, true))
          case _ =>
            logger.warn(
              s"""{"method":"parse", "error":"parser missing for given key,
                 |if parser is present then please check the matchRegExRule function for the same",
                 |"data":"${keyName}"}""".stripMargin)
            logger.warn(s"""{"method":"parse", "error":"no matching parser found","data":"${step}"}""")
            None
        }
      }).flatten
      ListParameter(paramMap.values.toList) :: codeList
    } else {
      Nil
    }
  }


  /**
   * Function separate out list of script code lines to individual steps
   *
   * @param lines   list of script code lines
   * @param acc     accumulator
   * @param accList accumulator list
   * @return
   */
  @tailrec
  private def scriptCodeSteps(lines: List[String], acc: List[String],
                              accList: List[String]): List[String] = {
    if (lines.isEmpty) {
      accList.reverse
    } else {
      lines match {
        case h :: _ =>
          val current = h
          val tail = lines.tail
          // check if the current one has any parameter the replace them with values
          val paramSubLine = current
          if (paramSubLine.contains("~>")) {
            //for a step last line
            val step = (paramSubLine :: acc).reverse.mkString(" ")
            scriptCodeSteps(tail, Nil, step :: accList)
          } else {
            scriptCodeSteps(tail, (paramSubLine :: acc), accList)
          }
        case Nil =>
          scriptCodeSteps(Nil, acc, accList)
      }
    }
  }


  /**
   * for given set of lines parse them using [[ParameterParser]]
   * It returns  map with parameter name as key and value as [[Parameter]]
   *
   * @param parameterLines
   * @return
   */
  private def parseParameterLines(parameterLines: String): Map[String, Parameter] = {
    // parse parameter lines
    if (!parameterLines.isEmpty) {
      val paramParser = new ParameterParser()
      paramParser.parse(parameterLines) match {
        case Some(listParam: ListParameter) =>
          listParam.list.map(param => param.paramName -> param).toMap
        case _ => Map.empty[String, Parameter]
      }
    } else {
      Map.empty[String, Parameter]
    }
  }

  /**
   * this functions is called only when last line was starting with parameters{
   * it returns parameters lines and rest of script code lines as list
   *
   * @param lines list of lines
   * @param acc
   * @param isParamEnd
   * @return
   */
  @tailrec
  private def findParameterLines(lines: List[String], acc: List[String], isParamEnd: Boolean): (String, List[String]) = {
    if (lines.isEmpty || isParamEnd) {
      (acc.reverse.mkString(" "), lines)
    } else {
      lines match {
        case h :: _ =>
          val next = lines.tail
          if (h.trim.equals("}")) {
            findParameterLines(next, h :: acc, true)
          } else {
            findParameterLines(next, h :: acc, false)
          }
      }
    }
  }
}

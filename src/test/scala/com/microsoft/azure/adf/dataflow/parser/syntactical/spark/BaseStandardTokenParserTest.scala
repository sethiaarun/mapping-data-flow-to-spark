package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import scala.util.matching.Regex

trait BaseStandardTokenParserTest extends BaseStandardTokenParser {

  /**
   * name
   * @return
   */
  def name(): String = ???

  /**
   * description of parser - friendly description detail
   *
   * @return
   */
  def description(): String = ???


  /**
   * This function defines matching regex rule when this parser should be applied
   * For example filter parser should be applied when we find filter( or source parser when source(output(
   *
   * @return
   */
  def matchRegExRule: Regex = ???

  /**
   * parse input using given parser
   *
   * @param input
   * @param parser
   * @tparam T
   * @return
   */
  def parse[T](input: String, parser: Parser[T]): Option[T] = {
    val tokens = new lexical.Scanner(input)
    val debug = System.getProperty("debug","false").toBoolean
    val parserExec: Parser[T] = if (debug) {
      log(parser)("testdebug")
    } else {
      parser
    }
    phrase(parserExec)(tokens) match {
      case Success(tree, _) => Option(tree)
      case ex: NoSuccess =>
        None
    }
  }
}

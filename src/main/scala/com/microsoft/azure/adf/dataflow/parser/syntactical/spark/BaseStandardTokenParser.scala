package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.semanticmodel.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.parser.lexical.SparkDataFlowLexical
import com.typesafe.scalalogging.Logger

import scala.util.matching.Regex
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

/**
 * Base Parser all other parser should extends from this
 */
trait BaseStandardTokenParser extends StandardTokenParsers {

  private val logger = Logger(getClass.getName)

  override val lexical = new SparkDataFlowLexical()

  //lexical delimiters
  lexical.delimiters ++= List("(", ")", ",", ":", "~>", "=", "==", "&&", "||",
    "===", "<=", ">=", "<", ">", "!=", "@", "{", "}", "$", "[", "]")
  //lexical reserved keywords , this can extend based on use case
  // don't add true and false as reserved keywords
  lexical.reserved ++= List("source", "output", "as", "select", "mapColumn",
    "filter", "union", "join", "parameters", "sink",
    "sort", "asc", "desc")


  /**
   * unimplemented function - name of parser
   * This is useful to cache list of parsers in Map with key name as name
   * So it should be unique for each parser
   *
   * @return
   */
  def name(): String

  /**
   * description of parser - friendly description detail
   *
   * @return
   */
  def description(): String

  /**
   * root parser for given keyword/flow step
   * This is used by parse function, ideally a parser should have one and only one parser
   * as public visibility
   *
   * @return
   */
  def root_parser: Parser[SparkCodeGenerator]

  /**
   * This function defines matching regex rule when this parser should be applied
   * For example filter parser should be applied when we find filter( or source parser when source(output(
   *
   * @return
   */
  def matchRegExRule: Regex

  /**
   * parse input using root_parser
   *
   * @param input
   */
  def parse(input: String): Option[SparkCodeGenerator] = {
    //if we want to log debug message for parsing
    val debug = System.getProperty("debug","false").toBoolean
    val parser: Parser[SparkCodeGenerator] = if (debug) {
      log(root_parser)(name())
    } else {
      root_parser
    }
    val tokens = new lexical.Scanner(input)
    phrase(parser)(tokens) match {
      case Success(tree, _) =>
        logger.debug(s"Parsed tree: ${tree}")
        Option(tree)
      case ex: NoSuccess =>
        logger.error(ex.toString)
        None
    }
  }
}

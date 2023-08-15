package com.microsoft.azure.adf.dataflow.parser

import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

import java.util.ServiceLoader
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * List of registered Parser
 */
trait ParserRegistry {

  // service loader to load parsers
  protected val parserFactory: ServiceLoader[BaseStandardTokenParser] = ServiceLoader.load(classOf[BaseStandardTokenParser],
    this.getClass.getClassLoader)

  val listOfParser: Map[String, BaseStandardTokenParser] =
    parserFactory.asScala.map(parserProvider => parserProvider.name() -> parserProvider).toMap

  /**
   * list of objects
   */
  lazy val listOfValues: List[BaseStandardTokenParser] = listOfParser.values.toList

  /**
   * get parser object
   *
   * @param reservedWord
   * @return
   */
  def getParserObject(nameOfParser: String): Option[BaseStandardTokenParser] = {
    listOfParser.get(nameOfParser)
  }

  /**
   * find matching parser name from the given input string
   *
   * @param input
   * @return
   */
  def findParserName(input: String): Option[String] = {
    val (keyName, isExist) = _findParserName(input, listOfValues, None, false)
    keyName
  }

  /**
   * find parser name from given input string, It is going to iterate each parser object and applies
   * matchRegExRule on the input string to find the matched parser. The first matched parser will be returned
   * In future if we can strengthen the matchRegExRule and chain of parser if require
   *
   * @param input     input data
   * @param parsers   list of parsers
   * @param keyName   return value from the tail recursion
   * @param isPresent boolean validation to let the tail recursion when to terminate
   * @return
   */
  @tailrec
  private def _findParserName(input: String, parsers: List[BaseStandardTokenParser],
                              keyName: Option[String], isPresent: Boolean): (Option[String], Boolean) = {
    if (parsers.isEmpty || (isPresent && keyName.isDefined)) {
      (keyName, isPresent)
    } else {
      val parser = parsers.head
      val keyExist = parser.matchRegExRule.findFirstMatchIn(input).isEmpty
      if (!keyExist) _findParserName(input, parsers.tail, Some(parser.name), !keyExist)
      else _findParserName(input, parsers.tail, None, keyExist)
    }
  }

}

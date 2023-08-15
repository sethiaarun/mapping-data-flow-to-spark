package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.model.union.UnionMapping
import com.microsoft.azure.adf.dataflow.model.util.ListKeyValueProperties
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.KeyValueColonSeparatedParser

import scala.util.matching.Regex

/**
 * Union Parser to parse
 * <<incoming stream>>, <<union with>> union(byName: true)~> <<output variable>>
 */
class UnionParser extends BaseStandardTokenParser with KeyValueColonSeparatedParser {

  /**
   * overall union parser
   *
   * @return
   */
  override def root_parser: Parser[UnionMapping] = (inStreamUnionWith_rule ~ union_rule ~ outputVarName_rule) ^^ {
    case inStream_UnionWith ~ unionProp ~ vn => UnionMapping(inStream_UnionWith._1, inStream_UnionWith._2, unionProp, vn)
  }

  /**
   * get incoming stream and union with
   *
   * @return
   */
  private def inStreamUnionWith_rule: Parser[(String, String)] = (ident ~ "," ~ ident) ^^ {
    case inStream ~ "," ~ unionWith => (inStream, unionWith)
  }

  /**
   * union parameters like byName:true, etc.
   *
   * @return
   */
  private def union_rule: Parser[ListKeyValueProperties] = ("union" ~ "(" ~> multiKvColonSep_rule <~ ")")

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bunion\\(".r
  }

  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "UnionParser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse union relationship"

}

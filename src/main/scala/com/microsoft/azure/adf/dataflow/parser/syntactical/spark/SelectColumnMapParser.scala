package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.semanticmodel.select.map.SelectColumnMapping
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.KeyValueColonSeparatedParser

import scala.util.matching.Regex

/**
 * parsers for select column mapping
 * inputvariable select(mapColumn(
 * VendorID    = VendorID
 * ),
 * skipDuplicateMapInputs: true,
 * skipDuplicateMapOutputs: true) ~> outputvariable
 */
class SelectColumnMapParser extends BaseStandardTokenParser
  with KeyValueColonSeparatedParser {

  /**
   * select col mapping parser
   *
   * @return
   */
  override def root_parser: Parser[SelectColumnMapping] =
    (ident ~ listMap_rule ~ "," ~ multiKvColonSep_rule ~ ")" ~ outputVarName_rule) ^^ {
      case source_identifier ~ column_map ~ "," ~ kv_prop ~ ")" ~ vn =>
        SelectColumnMapping(source_identifier, column_map, kv_prop, vn)
    }

  /**
   * match string for select(mapColumn(<<list of field mapping>>))
   *
   * @return
   */
  private def listMap_rule: Parser[Map[String, String]] = ("select" ~ "(" ~ "mapColumn" ~ "(" ~> mapFields_rule <~ ")")

  /**
   * select mapcolumn key and value - colName=mapped name
   *
   * @return
   */
  private def mapFields_rule: Parser[Map[String, String]] = rep1sep(colMap_rule, ",") ^^ {
    case options => options.map(v => (v._1 -> v._2)).toMap
  }

  /**
   * column mapping where column is mapped with other name or maybe the same name, in the case of the same name
   * the = sign will not be there, we will use same name as alias
   *
   * @return
   */
  private def colMap_rule: Parser[(String, String)] = ((ident ~ "=" ~ ident) ^^ { case s ~ "=" ~ d => (s, d) } |
    ident ^^ { case s => (s, s) })

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bselect\\(mapColumn\\b".r
  }

  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "SelectColumnMapParser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse Select query with map column"

}

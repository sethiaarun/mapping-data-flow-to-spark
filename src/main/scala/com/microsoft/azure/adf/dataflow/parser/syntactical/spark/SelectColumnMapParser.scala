package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.semanticmodel.select.map.SelectColumnMapping
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.{ColumnMapParser, KeyValueColonSeparatedParser}

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
  with KeyValueColonSeparatedParser
  with ColumnMapParser {

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
  private def listMap_rule: Parser[Map[String, String]] = ("select" ~ "(" ~ "mapColumn" ~ "(" ~> multiMapCol_rule <~ ")")


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
  override def name(): String = "SelectColumnMapTransformationParser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse Select query with map column"

}

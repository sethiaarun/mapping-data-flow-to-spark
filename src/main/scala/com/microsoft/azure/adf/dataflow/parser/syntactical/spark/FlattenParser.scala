package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.parser.syntactical.common.{ColumnMapParser, CommonUsableParser, KeyValueColonSeparatedParser}
import com.microsoft.azure.adf.dataflow.semanticmodel.flatten.DataFlowFlatten

import scala.util.matching.Regex

/**
 * Parse
 * source1 foldDown(unroll(col1,col2,...),
 * mapColumn(
 * destColName = sourceColName
 * sourceColName1,
 * sourceColName2
 * ),
 * skipDuplicateMapInputs: false,
 * skipDuplicateMapOutputs: false) ~> flatten1
 */

class FlattenParser extends BaseStandardTokenParser
  with CommonUsableParser
  with KeyValueColonSeparatedParser
  with ColumnMapParser {

  /**
   * unimplemented function - name of parser
   * This is useful to cache list of parsers in Map with key name as name
   * So it should be unique for each parser
   *
   * @return
   */
  override def name(): String = "FlattenTransformationParser"

  /**
   * description of parser - friendly description detail
   *
   * @return
   */
  override def description(): String = "Parser to parse Flatten transformation"

  /**
   * root parser for given keyword/flow step
   * This is used by parse function, ideally a parser should have one and only one parser
   * as public visibility
   *
   * @return
   */
  override def root_parser: Parser[DataFlowFlatten] =
    (source_rule ~ (multiUnroll_rule | unroll_rule) ~ mapColumn_rule ~ multiKvColonSep_rule ~ ")" ~ outputVarName_rule) ^^ {
      case source_identifier ~ unroll ~ column_map ~ kv_prop ~ ")" ~ vn =>
        DataFlowFlatten(source_identifier, unroll.fields, unroll.isMultiUnroll, column_map, kv_prop, vn)
    }

  private def source_rule: Parser[String] = ident <~ "foldDown" ~ "("

  /**
   * parse unroll(col1,col2,...),
   *
   * @return
   */
  private def unroll_rule: Parser[Unroll] = "unroll" ~ "(" ~> repsep(dotIdentifier_rule | ident, ",") <~ ")" ~ "," ^^ {
    case list => Unroll(list, false)
  }

  /**
   * parse unroll(col1,col2,...),
   *
   * @return
   */
  private def multiUnroll_rule: Parser[Unroll] = "unrollMultiple" ~ "(" ~>
    repsep(dotIdentifier_rule | ident, ",") <~ ")" ~ "," ^^ {
    case list => Unroll(list, true)
  }

  /**
   * parse
   * mapColumn(
   * destColName = sourceColName
   * sourceColName1,
   * sourceColName2
   * ),
   *
   * @return
   */
  private def mapColumn_rule: Parser[Map[String, String]] = "mapColumn" ~ "(" ~> multiMapCol_rule <~ ")" ~ ","

  /**
   * This function defines matching regex rule when this parser should be applied
   * For example filter parser should be applied when we find filter( or source parser when source(output(
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bfoldDown\\(unroll".r
  }

  /**
   * temp model for internal use
   *
   * @param fields
   * @param isMultiUnroll
   */
  private case class Unroll(fields: List[String], isMultiUnroll: Boolean)
}

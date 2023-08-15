package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import com.microsoft.azure.adf.dataflow.model.sort.SortType.SortType
import com.microsoft.azure.adf.dataflow.model.sort.{DataFlowSort, SortField, SortType}
import com.microsoft.azure.adf.dataflow.parser.syntactical.common.{CommonUsableParser, KeyValueColonSeparatedParser}

import scala.util.matching.Regex

/**
 * Parser to parse Sort operation , convert it to the Spark Code generation model object
 * cityselect sort(desc(City, true),
 * asc(State, true),
 * caseInsensitive: true,
 * partitionLevel: true) ~> sort1
 *
 */
class SortParser extends BaseStandardTokenParser
  with CommonUsableParser
  with KeyValueColonSeparatedParser {

  /**
   * input sort( <sort fields>*,
   * sort properties*
   * ) ~> <output variable>
   *
   * @return
   */
  override def root_parser: Parser[DataFlowSort] = (ident ~ "sort" ~ "(" ~ listSortMap_rule ~ "," ~ multiKvColonSep_rule ~ ")" ~ outputVarName_rule) ^^ {
    case inputVar ~ "sort" ~ "(" ~ lstSortFields ~ "," ~ sortProperties ~ ")" ~ vr =>
      DataFlowSort(inputVar, lstSortFields, sortProperties, vr)
  }

  /**
   * get sorting field with type of sort
   * desc(City, true)
   *
   * @return
   */
  private def sortMap_rule: Parser[SortField] = (("desc" | "asc") ~ "(" ~ ident ~ "," ~ ident <~ ")") ^^ {
    case sortType ~ "(" ~ field ~ "," ~ nullFirst => SortField(SortType.getType(sortType), field, nullFirst.toBoolean)
  }

  /**
   * list of sort fields
   *
   * @return
   */
  private def listSortMap_rule: Parser[List[SortField]] = rep1sep(sortMap_rule, ",")


  /**
   * sort type using asc or desc
   *
   * @return
   */
  private def sortType_rule: Parser[SortType] = ("desc" | "asc") ^^ { case sortType => SortType.getType(sortType) }

  /**
   * This function defines matching regex rule when this parser should be applied
   *
   * @return
   */
  override def matchRegExRule: Regex = {
    "\\bsort\\(".r
  }

  /**
   * name of parser
   *
   * @return
   */
  override def name(): String = "SortParser"

  /**
   * description of parser
   *
   * @return
   */
  override def description(): String = "Parser to parse sort transformation"

}

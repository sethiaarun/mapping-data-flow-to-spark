package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

/**
 * Column map parser where source and destination columns are mapped
 * for example
 * destColName1 = sourceColName1 or format like
 * sourceColName or format like
 * destColName2 = sourceColName2
 */
trait ColumnMapParser extends CommonUsableParser {
  this: BaseStandardTokenParser =>

  /**
   * list of source and destination column mapping, separated by ","
   *
   * @return
   */
  protected def multiMapCol_rule: Parser[Map[String, String]] = rep1sep(colMap_rule, ",") ^^ {
    case options => options.map(v => (v._1 -> v._2)).toMap
  }

  /**
   * column mapping where column is mapped with other name or maybe the same name, in the case of the same name
   * the = sign will not be there, we will use same name as alias
   * destColName1 = sourceColName1 or format like
   * destColName1 = sourcefield1.subfield1.subfield2 -> this case can occur in json format
   * sourceColName or format like
   * destColName2 = sourceColName2
   *
   * @return
   */
  protected def colMap_rule: Parser[(String, String)] = ((ident ~ "=" ~ dotIdentifier_rule) ^^ { case d ~ "=" ~ s => (d, s) } |
    ident ^^ { case s => (s, s) })
}

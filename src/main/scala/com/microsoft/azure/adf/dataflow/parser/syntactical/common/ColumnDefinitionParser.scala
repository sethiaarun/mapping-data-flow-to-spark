package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.model.source.{ColumnDefinition, ColumnDefinitionList}
import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

/**
 * This parses where grammar is <identifier> as <datatype>
 */
trait ColumnDefinitionParser {
  this: BaseStandardTokenParser =>

  /**
   * list of field defined in this format
   * <<name1>> as <<datatype1>>,
   * <<name2>> as <<datatype2>>
   *
   * @return
   */
  protected def listColumnDefinition_rule: Parser[ColumnDefinitionList] = rep1sep(columnDefinition_rule, ",") ^^ {
    case list => ColumnDefinitionList(list)
  }

  /**
   * output field with name and type field as type
   *
   * columnIdentifier AS (inlineDatatype)
   * @return
   */
  protected def columnDefinition_rule: Parser[ColumnDefinition] = (ident ~ "as" ~ (typeDecimal_rule | ident)) ^^ {
    case f ~ "as" ~ t => ColumnDefinition(f, t)
  }

  /**
   * decimal data type like decimal(18,2)
   *
   * @return
   */
  private def typeDecimal_rule: Parser[String] = (ident ~ "(" ~ numericLit ~ "," ~ numericLit ~ ")") ^^ {
    case ty ~ "(" ~ n1 ~ "," ~ n2 ~ ")" => List(ty, "(", n1, ",", n2, ")").mkString
  }
}

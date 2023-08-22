package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser
import com.microsoft.azure.adf.dataflow.semanticmodel.column.{Column, ColumnDefinition, ComplexColumn}

/**
 * This parses where grammar is <identifier> as <datatype>
 * or complex such as
 * goods as (customers as string, orders as (orderId as integer, orderTotal as double, shipped as (orderItems as (itemName as string, itemQty as integer)[]))[], trade as boolean)
 */
trait ColumnDefinitionParser {
  this: BaseStandardTokenParser =>


  /**
   * List of column definition
   * It may contain [[ColumnDefinition]] like customer as String
   * or [[ComplexColumn]] like
   * goods as (customers as string, orders as (orderId as integer, orderTotal as double)[], trade as boolean)
   *
   * @return
   */
  def colDefinition_rule: Parser[List[Column]] = repsep((simpleColumnNameType | complexColumnDataType), ",")

  def simpleColumnNameType: Parser[ColumnDefinition] = (columnName_rule ~ (arrayDataType_rule | literalDataType_rule) ^^ { case name ~ tp => ColumnDefinition(name, tp) })

  /**
   * complex column data type such as
   * customer as (name as string, id as int, address as (city as string)[])
   *
   * @return
   */
  def complexColumnDataType: Parser[ComplexColumn] = (((columnName_rule ~ "(" ~ colDefinition_rule ~ ")" <~ "[" ~ "]") | (columnName_rule ~ "(" ~ colDefinition_rule ~ ")"))
    ^^ { case a ~ "(" ~ t ~ ")" => ComplexColumn(a, t) }
    )

  /**
   * column data type as array like customer as string[]
   *
   * @return
   */
  protected def arrayDataType_rule: Parser[String] = (literalDataType_rule <~ "[" ~ "]") ^^ {
    case ty => ty + "[]"
  }

  private def columnName_rule: Parser[String] = ident <~ "as"

  /**
   * Literal data type like String, float, double (18,2), etc.
   *
   * @return
   */
  private def literalDataType_rule: Parser[String] = (typeDecimal_rule | ident)

  /**
   * decimal data type like decimal(18,2)
   *
   * @return
   */
  private def typeDecimal_rule: Parser[String] = (ident ~ "(" ~ numericLit ~ "," ~ numericLit ~ ")") ^^ {
    case ty ~ "(" ~ n1 ~ "," ~ n2 ~ ")" => List(ty, "(", n1, ",", n2, ")").mkString
  }



}

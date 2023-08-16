package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

trait OperandParser extends CommonUsableParser {
  this: BaseStandardTokenParser =>

  /**
   * source and field of that source concatenated by @ symbol
   * for example sourcestram@field1
   * Identifier @ Identifier
   *
   * @return
   */
  protected def columnOperand_rule: Parser[String] = (ident ~ "@" ~ ident) ^^ {
    case source ~ "@" ~ fieldName => s"""${source}@${fieldName}"""
  }

  /**
   * filter dynamic variable which are prefixed by $
   * these scenarios are exist when user has defined parameters in the
   * script code and used them part of flow steps
   * $Identifier
   *
   * @return
   */
  protected def parameterOperand_rule: Parser[String] = ("$" ~ ident) ^^ { case "$" ~ identifier => identifier }


  /**
   * COLON: (ident | stringLit | numericLit | emptyBrackets_rule)
   *
   * @return
   */
  protected def transientOperand_rule: Parser[String] = (":" ~> (ident | stringLit | numericLit | emptyBrackets_rule) ^^ {
    case v => v
  })

  /**
   * identifier prefixed by $ sign when values are given by properties/parameters
   * Key: ($identifier)
   *
   * @return
   */
  protected def propertiesOperand_rule: Parser[String] = ("(" ~> parameterOperand_rule <~ ")") ^^ {
    case identifier => "$" + identifier
  }
}

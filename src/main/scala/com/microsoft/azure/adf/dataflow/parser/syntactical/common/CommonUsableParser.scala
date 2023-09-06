package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

/**
 * Common reusable parser
 */
trait CommonUsableParser {
  this: BaseStandardTokenParser =>

  /**
   * Property value is given in from of []
   *
   * like preCommands:[]
   *
   * @return
   */
  protected def emptyBrackets_rule: Parser[String] = ("[" ~ "]") ^^^ {
    ""
  }

  /**
   * output variable name used after ~>
   *
   * @return
   */
  protected def outputVarName_rule: Parser[String] = ("~>" ~> ident) ^^ { case v => v }


  /**
   * boolean parser
   *
   * @return
   */
  protected def boolean_rule: Parser[Boolean] = ("true" | "false") ^^ { case bln => bln.toBoolean }

  /**
   * dot identifier like a.b or a.b.c
   * this can be a use case for column name in json format
   *
   * @return
   */
  protected def dotIdentifier_rule: Parser[String] = repsep(ident, ".") ^^ { case l => l.mkString(".") }
}

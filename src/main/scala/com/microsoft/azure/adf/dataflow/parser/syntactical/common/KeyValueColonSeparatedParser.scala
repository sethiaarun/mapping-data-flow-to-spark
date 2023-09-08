package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.semanticmodel.util.{KeyValueProperties, ListKeyValueProperties}
import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParser

/**
 * Key value are separated by colon
 * Grammar key:value
 */
trait KeyValueColonSeparatedParser extends CommonUsableParser
  with OperandParser {
  this: BaseStandardTokenParser =>

  /**
   * list of properties configured for source like format: 'parquet', validateSchema: false, etc.
   * output will be key abd value separated by ":"
   *
   * @return
   */
  protected def multiKvColonSep_rule: Parser[ListKeyValueProperties] =
    rep1sep(kvColonSep_rule, ",") ^^ { case list => ListKeyValueProperties(list) }

  /**
   * Single key value colon separated
   *
   * @return
   */
  private def kvColonSep_rule: Parser[KeyValueProperties] = (kvColonProp_rule | kvColonSepParam_rule)

  /**
   * Single key value colon separated
   *
   * @return
   */
  private def kvColonProp_rule: Parser[KeyValueProperties] = (ident ~ transientOperand_rule) ^^ {
    case k ~ v => KeyValueProperties(k, v, false)
  }

  /**
   * this is key value properties separated by ":" and it is a parameter to be replaced by runtime
   * like PropertyName:($PropValue)
   *
   * @return
   */
  private def kvColonSepParam_rule: Parser[KeyValueProperties] = (ident ~ ":" ~ propertiesOperand_rule) ^^ {
    case k ~ ":" ~ v => KeyValueProperties(k, v, true)
  }
}

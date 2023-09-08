package com.microsoft.azure.adf.dataflow.parser.syntactical.common

import com.microsoft.azure.adf.dataflow.parser.syntactical.spark.BaseStandardTokenParserTest
import com.microsoft.azure.adf.dataflow.semanticmodel.SparkCodeGenerator
import com.microsoft.azure.adf.dataflow.semanticmodel.column.{Column, ColumnDefinition, ColumnDefinitionList}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should


/**
 * Test cases for Column Definition Parser
 */
class ColumnDefinitionParserSpec extends AnyFlatSpec
  with should.Matchers
  with PrivateMethodTester {

  class ColumnDefinitionParserTest extends BaseStandardTokenParserTest
    with ColumnDefinitionParser {

    override def root_parser: Parser[SparkCodeGenerator] = ???

    // override protected functions, since they can be invoked through
    // the implementation of its subclasses.
    override def colDefinition_rule: Parser[List[Column]] = super.colDefinition_rule

    override def arrayDataType_rule: Parser[String] = super.arrayDataType_rule
  }

  val columnDefParserTest = new ColumnDefinitionParserTest()

  it should "parse literal column type as string definition" in {
    val input = "name as string"
    assert(columnDefParserTest.parse(input, columnDefParserTest.colDefinition_rule).isDefined)
  }
  it should "parse literal column type as decimal(18,2) definition" in {
    val input = "name as decimal(18,2)"
    assert(columnDefParserTest.parse(input, columnDefParserTest.colDefinition_rule).isDefined)
  }
  it should "parse literal column type as array definition" in {
    val input = "string[]"
    assert(columnDefParserTest.parse(input, columnDefParserTest.arrayDataType_rule).isDefined)
  }
  it should "parse list of column definition" in {
    val input =
      """
        |DeliveryDateKey as timestamp,
        |		SalespersonKey as integer,
        |		WWIInvoiceID as integer,
        |		Description as string,
        |		Package as string,
        |		Quantity as integer,
        |		UnitPrice as decimal(18,2),
        |		TaxRate as decimal(18,3),
        |   customers as string[]
        |""".stripMargin
    assert(columnDefParserTest.parse(input, columnDefParserTest.colDefinition_rule).isDefined)
  }
  it should "parse complex data type" in {
    val input =
      """
        |goods as (customers as string, orders as (orderId as integer, orderTotal as double, shipped as (orderItems as (itemName as string, itemQty as integer)[]))[], trade as boolean),
        |WWIInvoiceID as integer
        |""".stripMargin
    assert(columnDefParserTest.parse(input, columnDefParserTest.colDefinition_rule).isDefined)
  }

}

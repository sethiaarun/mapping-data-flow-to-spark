package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for the source parser
 */
class SourceParserSpec extends AnyFlatSpec with should.Matchers {
  val sourceParser = new SourceParser()
  it should "parse the source transformation with parameters" in {
    val input =
      """
        |source(output(
        |		SaleKey as long,
        |		CityKey as integer,
        |		CustomerKey as integer,
        |		BillToCustomerKey as integer,
        |		StockItemKey as integer,
        |		InvoiceDateKey as timestamp,
        |		DeliveryDateKey as timestamp,
        |		SalespersonKey as integer,
        |		WWIInvoiceID as integer,
        |		Description as string,
        |		Package as string,
        |		Quantity as integer,
        |		UnitPrice as decimal(18,2),
        |		TaxRate as decimal(18,3),
        |		TotalExcludingTax as decimal(18,2),
        |		TaxAmount as decimal(18,2),
        |		Profit as decimal(18,2),
        |		TotalIncludingTax as decimal(18,2),
        |		TotalDryItems as integer,
        |		TotalChillerItems as integer,
        |		LineageKey as integer
        |	),
        |	allowSchemaDrift: true,
        |	validateSchema: false,
        |	ignoreNoFilesFound: false,
        |	format: 'parquet',
        |	fileSystem: ($Container),
        |	folderPath: ($SalesFolderPath)) ~> sales
        |""".stripMargin
    assert(sourceParser.parse(input).isDefined)
  }
  it should "parse the source transformation without parameters" in {
    val input =
      """
        |source(output(
        |		SaleKey as long,
        |		CityKey as integer,
        |		CustomerKey as integer,
        |		BillToCustomerKey as integer,
        |		StockItemKey as integer,
        |		InvoiceDateKey as timestamp,
        |		DeliveryDateKey as timestamp,
        |		SalespersonKey as integer,
        |		WWIInvoiceID as integer,
        |		Description as string,
        |		Package as string,
        |		Quantity as integer,
        |		UnitPrice as decimal(18,2),
        |		TaxRate as decimal(18,3),
        |		TotalExcludingTax as decimal(18,2),
        |		TaxAmount as decimal(18,2),
        |		Profit as decimal(18,2),
        |		TotalIncludingTax as decimal(18,2),
        |		TotalDryItems as integer,
        |		TotalChillerItems as integer,
        |		LineageKey as integer
        |	),
        |	allowSchemaDrift: true,
        |	validateSchema: false,
        |	ignoreNoFilesFound: false,
        |	format: 'parquet') ~> sales
        |""".stripMargin
    assert(sourceParser.parse(input).isDefined)
  }
}

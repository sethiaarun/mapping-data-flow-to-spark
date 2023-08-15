package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for the select column map parser
 */
class SelectColumnMapParserSpec extends AnyFlatSpec with should.Matchers {
  val selectColMapParser = new SelectColumnMapParser()
  it should "parse the select column transformation with parameters" in {
    val input =
      """
       salesfilter select(mapColumn(
        |		CityKey,
        |		InvoiceDate = InvoiceDateKey,
        |		DeliveryDate = DeliveryDateKey,
        |		Salesperson = SalespersonKey,
        |		Package,
        |		Quantity,
        |		UnitPrice,
        |		TaxRate,
        |		TotalWithoutTax = TotalExcludingTax,
        |		TaxAmount,
        |		Profit,
        |		TotalWithTax = TotalIncludingTax
        |	),
        |	skipDuplicateMapInputs: true,
        |	skipDuplicateMapOutputs: true) ~> salesselect
        |""".stripMargin
    assert(selectColMapParser.parse(input).isDefined)
  }

}

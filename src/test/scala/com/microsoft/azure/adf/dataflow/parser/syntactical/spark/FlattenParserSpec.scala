package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for the flatten parser
 */
class FlattenParserSpec extends AnyFlatSpec with should.Matchers {
  val flattenParser = new FlattenParser()
  it should "parse the flatten transformation input" in {
    val input =
      """
        |source1 foldDown(unroll(goods.orders, goods.orders),
        |    mapColumn(
        |        orderId = goods.orders.orderId,
        |        customers = goods.customers,
        |        itemName = goods.orders.shipped.orderItems.itemName
        |    ),
        |    skipDuplicateMapInputs: false,
        |    skipDuplicateMapOutputs: false) ~> flatten1
        |""".stripMargin
    assert(flattenParser.parse(input).isDefined)
  }
  it should "parse the flatten transformation input with one column unroll" in {
    val input =
      """
        |source1 foldDown(unroll(goods.customers),
        |    mapColumn(
        |        customers = goods.customers,
        |        name
        |    ),
        |    skipDuplicateMapInputs: false,
        |    skipDuplicateMapOutputs: false) ~> flatten1
        |""".stripMargin
    assert(flattenParser.parse(input).isDefined)
  }
  it should "parse the flatten transformation with no map column" in {
    val input =
      """
        |source1 foldDown(unroll(goods.customers),
        |    skipDuplicateMapInputs: false,
        |    skipDuplicateMapOutputs: false) ~> flatten1
        |""".stripMargin
    assert(flattenParser.parse(input).isEmpty)
  }

}

package com.microsoft.azure.adf.dataflow.parser.syntactical.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

/**
 * test cases for the join parser
 */
class JoinParserSpec extends AnyFlatSpec with should.Matchers {
  val parser = new JoinParser()
  it should "parse the simple inner join" in {
    val input =
      """
        | salesselect, cityselect join((salesselect@CityKey) == (cityselect@CityKey), 	joinType:'left', 	matchType:'exact', 	ignoreSpaces: false, 	broadcast: 'auto')~> joinwithcity
        |""".stripMargin
    assert(parser.parse(input).isDefined)
  }
  it should "parse should throw error because output is missing" in {
    val input =
      """
        | master1, master2 join(ltrim(rtrim(lower(master2@name))) == ltrim(rtrim(lower(master1@Name))),
        |	joinType:'inner',
        |	matchType:'exact',
        |	ignoreSpaces: false,
        |	broadcast: 'auto')
        |""".stripMargin
    assert(!parser.parse(input).isDefined)
  }
  it should "parse should throw error because join keyword is missing" in {
    val input =
      """
        | master1, master2 (ltrim(rtrim(lower(master2@name))) == ltrim(rtrim(lower(master1@Name))),
        |	joinType:'inner',
        |	matchType:'exact',
        |	ignoreSpaces: false,
        |	broadcast: 'auto')~> measure
        |""".stripMargin
    assert(!parser.parse(input).isDefined)
  }
}

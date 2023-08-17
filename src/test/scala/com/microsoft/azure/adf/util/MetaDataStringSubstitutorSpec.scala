package com.microsoft.azure.adf.util

import org.apache.commons.text.StringSubstitutor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.io.Source
import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * Test cases for Metadata String Substitution
 */
class MetaDataStringSubstitutorSpec extends AnyFlatSpec with should.Matchers {
  it should "replace placeholder" in {
    val stream = Source.fromResource("code/notebookmetadata/PySparkFabric")
    val input: List[String] = stream.getLines().toList
    val text = input.mkString("\n")
    stream.close()

    val mapReplace = Map("lakeHouseId" -> "4454454545454-6556-5656-7775-1ewrtyu789",
      "lakeHouseName" -> "TestLakeHouse",
      "workSpaceId" -> "6767jio-81be-4545-8e61-565655hjku")

    val resultStream = Source.fromResource("testresults/code/notebookmetadata/pysparkmetadataoutput")
    val result: List[String] = resultStream.getLines().toList
    val resultText = result.mkString("\n")
    resultStream.close()
    assert(StringSubstitutor.replace(text,mapReplace.asJava)===resultText)
  }
  it should "replace placeholder with no value" in {
    val stream = Source.fromResource("code/notebookmetadata/PySparkFabric")
    val input: List[String] = stream.getLines().toList
    val text = input.mkString("\n")
    stream.close()

    val mapReplace = Map("lakeHouseId" -> "",
      "lakeHouseName" -> "",
      "workSpaceId" -> "")

    val resultStream = Source.fromResource("testresults/code/notebookmetadata/pysparkmetadataemptyoutput")
    val result: List[String] = resultStream.getLines().toList
    val resultText = result.mkString("\n")
    resultStream.close()
    assert(StringSubstitutor.replace(text,mapReplace.asJava)===resultText)
  }

}

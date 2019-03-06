package com.datio.kirby.transformation.column

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CharacterTrimmerTest  extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory{


  feature("Adjust field to correct format") {
    scenario("Remove leading zeros") {
      Given("config")



      val config = ConfigFactory.parseString(
        s"""
           |{
           |        field = "columnTest"
           |        type = "charactertrimmer"
           |        charactertrimmer = "0"
           | }
         """.stripMargin)

      Given("column to parse")
      import spark.implicits._
      val df = List("0000182", "00008", "1000012", "002212").toDF("columnTest")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("value should be cast to alphanumeric removing zeros at the beginning")

      val resultList: Array[Row] = dfResult.select("columnTest").collect


      resultList.contains(Row("182")) shouldBe true
      resultList.contains(Row("8")) shouldBe true
      resultList.contains(Row("1000012")) shouldBe true
      resultList.contains(Row("2212")) shouldBe true
      resultList.contains(Row("0000")) shouldBe false

      resultList.length shouldBe 4

      assert(
        dfResult.schema.fields.tail.forall(
          _.dataType === StringType
        )
      )
    }
  }
}
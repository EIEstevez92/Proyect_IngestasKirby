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
class CommaDelimiterTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Cast to CommaDelimiter filling with the suitable character") {
    scenario("Convert a column to decimal number") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "columnTest"
           |        type = "commaDelimiter"
           |        lengthDecimal = 3
           |        separatorDecimal= "."
           |      }
        """.stripMargin)

      Given("column to parse")
      import spark.implicits._

      val df = List("18243", "8", "127", "2212").toDF("columnTest")


      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("value should be cast to decimal inserting separator at desired position")

      val resultList: Array[Row] = dfResult.select("columnTest").collect


      resultList.contains(Row("18.243")) shouldBe true
      resultList.contains(Row("0.008")) shouldBe true
      resultList.contains(Row("0.127")) shouldBe true
      resultList.contains(Row("2.212")) shouldBe true
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
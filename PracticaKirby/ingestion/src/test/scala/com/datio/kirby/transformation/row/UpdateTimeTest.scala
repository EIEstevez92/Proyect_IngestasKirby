package com.datio.kirby.transformation.row

import java.sql.Timestamp

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class UpdateTimeTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Update or create a field with current timestamp") {
    scenario("field not exists") {
      Given("config")
      val config = ConfigFactory.parseString(
        """
          |{
          |  type : "setCurrentDate"
          |  field : "updateTime"
          |}
        """.stripMargin)

      Given("field to update")

      import spark.implicits._
      val columnsToUpdate = List(3, 4, 5, 6)
      val df = columnsToUpdate.toDF("text")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("must be a new field called 'updateTime'")

      val resultListCount: Array[Row] = dfResult.select("updateTime").groupBy("updateTime").count().collect

      resultListCount.length shouldBe 1
      resultListCount(0).get(1) shouldBe 4
      resultListCount(0).get(0).isInstanceOf[Timestamp] shouldBe true

    }

    scenario("field exists") {
      Given("config")
      val config = ConfigFactory.parseString(
        """
          |{
          |  type : "setCurrentDate"
          |  field : "updateTime"
          |}
        """.stripMargin)

      Given("field to update")
      val startTime = java.sql.Timestamp.from(java.time.Instant.now)

      import spark.implicits._
      val columnsToUpdate = List((3, startTime), (4, startTime), (5, startTime), (6, startTime))
      val df = columnsToUpdate.toDF("text", "updateTime")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("must be a new field called 'updateTime'")

      val resultListCount: Array[Row] = dfResult.select("updateTime").groupBy("updateTime").count().collect

      resultListCount.length shouldBe 1
      resultListCount(0).get(1) shouldBe 4
      resultListCount(0).get(0).isInstanceOf[Timestamp] shouldBe true
      resultListCount(0).get(0).asInstanceOf[Timestamp].after(startTime) shouldBe true
    }

    scenario("field exists with strange characters") {
      Given("config")
      val config = ConfigFactory.parseString(
        """
          |{
          |  type : "setCurrentDate"
          |  field : "update(time)"
          |}
        """.stripMargin)

      Given("field to update")
      val startTime = java.sql.Timestamp.from(java.time.Instant.now)

      import spark.implicits._
      val columnsToUpdate = List((3, startTime), (4, startTime), (5, startTime), (6, startTime))
      val df = columnsToUpdate.toDF("text", "update(time)")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("must be a new field called 'updateTime'")

      val resultListCount: Array[Row] = dfResult.select("update(time)").groupBy("update(time)").count().collect

      resultListCount.length shouldBe 1
      resultListCount(0).get(1) shouldBe 4
      resultListCount(0).get(0).isInstanceOf[Timestamp] shouldBe true
      resultListCount(0).get(0).asInstanceOf[Timestamp].after(startTime) shouldBe true
    }
  }

}

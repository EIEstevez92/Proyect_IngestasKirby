package com.datio.kirby.transformation.column

import java.sql.Date
import java.util.Calendar

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ExtractInfoFromDateTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  private val calendar = Calendar.getInstance()

  feature("Extract info from date") {
    scenario("extract year") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "year"
           |        type = "extractinfofromdate"
           |        dateField = "date"
           |        info = "year"
           |      }
        """.stripMargin)


      calendar.set(Calendar.YEAR, 2015)
      val day: Date = new Date(calendar.getTime.getTime)

      Given("column to parse")

      val columnToParse = Row(day) :: Row(day) :: Row(day) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("date", DateType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      Then("value should be copied into new column")

      val resultList: Array[Row] = dfResult.select("year").collect

      assert(
        resultList.forall(
          _.getString(0) === "2015"
        )
      )

      resultList.length shouldBe 3

    }

    scenario("extract month") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "month"
           |        type = "extractinfofromdate"
           |        dateField = "date"
           |        info = "month"
           |      }
        """.stripMargin)


      calendar.set(Calendar.MONTH, 2)
      val day: Date = new Date(calendar.getTime.getTime)

      Given("column to parse")

      val columnToParse = Row(day) :: Row(day) :: Row(day) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("date", DateType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      Then("value should be copied into new column")

      val resultList: Array[Row] = dfResult.select("month").collect

      assert(
        resultList.forall(
          _.getString(0) === "2"
        )
      )

      resultList.length shouldBe 3
    }

    scenario("extract day") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "day"
           |        type = "extractinfofromdate"
           |        dateField = "date"
           |        info = "day"
           |      }
        """.stripMargin)


      calendar.set(Calendar.DAY_OF_MONTH, 16)
      val day: Date = new Date(calendar.getTime.getTime)

      Given("column to parse")

      val columnToParse = Row(day) :: Row(day) :: Row(day) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("date", DateType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      Then("value should be copied into new column")

      val resultList: Array[Row] = dfResult.select("day").collect

      assert(
        resultList.forall(
          _.getString(0) === "16"
        )
      )

      resultList.length shouldBe 3

    }

    scenario("fail when date is malformed") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "day"
           |        type = "extractinfofromdate"
           |        dateField = "date"
           |        info = "day"
           |      }
        """.stripMargin)


      calendar.set(Calendar.DAY_OF_MONTH, 16)
      val day: Date = new Date(calendar.getTime.getTime)

      Given("column to parse")

      val columnToParse = Row(null) :: Row(null) :: Row(null) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("date", DateType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      Then("value should be copied into new column")

      val resultList: Array[Row] = dfResult.select("day").collect

      assert(
        resultList.forall(
          _.get(0) === null
        )
      )

      resultList.length shouldBe 3

    }

  }

}

package com.datio.kirby.transformation.column

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LeftPaddingTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Cast to LeftPadding filling with the suitable character") {
    scenario("fill with zeros a correct office number") {
      Given("config")
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "columnWithValueCopied"
           |        type = "leftpadding"
           |        lengthDest = 4
           |        fillCharacter= 0
           |      }
        """.stripMargin)

      Given("column to parse")
      val columnToParse = Seq(Row(182), Row(8), Row(null), Row(12), Row(2212))
      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(Seq(StructField("columnWithValueCopied", IntegerType)))
      )

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("value should be cast to alphanumeric filling with zeros at the beginning")
      dfResult.select("columnWithValueCopied").collect.toSet shouldBe Set(Row("0182"), Row("0008"), Row("0012"), Row("2212"), Row(null))
      dfResult.schema.map(_.dataType) shouldBe Seq(StringType)
    }
  }

  scenario("fill with zeros a correct office number using default for null") {
    Given("config")
    val config = ConfigFactory.parseString(
      s"""
         |      {
         |        field = "columnWithValueCopied"
         |        type = "leftpadding"
         |        lengthDest = 4
         |        fillCharacter= 0
         |        nullValue= "nullValue"
         |      }
        """.stripMargin)

    Given("column to parse")
    val columnToParse = Seq(Row(182), Row(8), Row(null), Row(12), Row(2212))
    val df = spark.createDataFrame(columnToParse.asJava,
      StructType(Seq(StructField("columnWithValueCopied", IntegerType)))
    )

    When("apply transformations")
    val dfResult = readTransformation(config)(spark).transform(df)

    Then("value should be cast to alphanumeric filling with zeros at the beginning")
    dfResult.select("columnWithValueCopied").collect.toSet shouldBe Set(Row("0182"), Row("0008"), Row("0012"), Row("2212"), Row("nullValue"))
    dfResult.schema.map(_.dataType) shouldBe Seq(StringType)
  }


  feature("Cast to LeftPadding using toalphanumeric key") {
    scenario("fill with zeros a correct office number") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "columnWithValueCopied"
           |        type = "toalphanumeric"
           |        lengthDest = 4
           |        fillCharacter= 0
           |      }
        """.stripMargin)

      Given("column to parse")
      val columnToParse = Seq(Row(182), Row(8), Row(12), Row(2212))
      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(Seq(StructField("columnWithValueCopied", IntegerType)))
      )

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df).select("columnWithValueCopied").collect

      Then("value should be cast to alphanumeric filling with zeros at the beginning")
      dfResult.toSet shouldBe Set(Row("0182"), Row("0008"), Row("0012"), Row("2212"))
      dfResult.length shouldBe 4

    }
  }
}

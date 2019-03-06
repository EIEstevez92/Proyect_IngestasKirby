package com.datio.kirby.transformation.column

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CopyColumnTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Return correct parser") {
    scenario("copy column integer") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "columnWithValueCopied"
           |        type = "copycolumn"
           |        copyField = "columnToCopy"
           |      }
        """.stripMargin)

      Given("column to parse")

      val columnToParse = Row(0) :: Row(1) :: Row(2) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("columnToCopy", IntegerType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("value should be copied into new column")

      val resultList: Array[Row] = dfResult.select("columnWithValueCopied").collect

      resultList.contains(Row(0)) shouldBe true
      resultList.contains(Row(1)) shouldBe true
      resultList.contains(Row(2)) shouldBe true
      resultList.contains(Row(3)) shouldBe false

      resultList.length shouldBe 3

      assert(
        dfResult.schema.fields.tail.forall(
          _.dataType === IntegerType
        )
      )

    }

    scenario("copy column with cast") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "columnWithValueCopied"
           |        type = "copycolumn"
           |        copyField = "columnToCopy"
           |        defaultType = "double"
           |      }
        """.stripMargin)

      Given("column to parse")

      val columnToParse = Row(0) :: Row(1) :: Row(2) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("columnToCopy", IntegerType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("value should be copied into new column and casted")

      val resultList: Array[Row] = dfResult.select("columnWithValueCopied").collect

      assert(
        dfResult.schema.fields.tail.forall(
          _.dataType === DoubleType
        )
      )

      resultList.contains(Row(0D)) shouldBe true
      resultList.contains(Row(1D)) shouldBe true
      resultList.contains(Row(2D)) shouldBe true
      resultList.contains(Row(3D)) shouldBe false

      resultList.length shouldBe 3

    }
  }

}

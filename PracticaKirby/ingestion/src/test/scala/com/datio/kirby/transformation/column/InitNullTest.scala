package com.datio.kirby.transformation.column

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class InitNullTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Return correct parser") {
    scenario("check correct parse init int nulls") {
      Given("config")

      val initNullsConfig = ConfigFactory.parseString(
        """
          |  {
          |    field = "text"
          |    type = "initNulls"
          |    default = "0"
          |  }
          |  """.stripMargin)

      Given("column to parse")

      val columnToParse: Seq[Row] = Row(3) :: Row(4) :: Row(None.orNull) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", IntegerType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(initNullsConfig)(spark).transform(df)

      Then("text should be change by catalog")

      val resultList: Array[Row] = dfResult.select("text").collect

      resultList.contains(Row("3")) shouldBe true
      resultList.contains(Row("4")) shouldBe true
      resultList.contains(Row("0")) shouldBe true
      resultList.contains(Row(None.orNull)) shouldBe false
      resultList.length shouldBe 3

    }
  }
}
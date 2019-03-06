package com.datio.kirby.transformation.column

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
class TrimmerTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("trim column") {
    scenario("trim strings by different sides") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |    transformations = [
           |      {
           |        field = "trim"
           |        type = "trim"
           |        trimType = "both"
           |      },
           |      {
           |        field = "trimRight"
           |        type = "trim"
           |        trimType = "right"
           |      },
           |      {
           |        field = "trimLeft"
           |        type = "trim"
           |        trimType = "left"
           |      },
           |      {
           |        field = "trimInvalidConfig"
           |        type = "trim"
           |        trimType = "unknown"
           |      }
           |    ]
        """.stripMargin)

      Given("column to parse")

      val testData = "    strTest      "
      val columnToParse = Row(testData, testData, testData, testData) :: Nil
      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("trim", StringType) ::
            StructField("trimRight", StringType) ::
            StructField("trimLeft", StringType) ::
            StructField("trimInvalidConfig", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = config.getConfigList("transformations").asScala
        .foldLeft(df)((dfA, conf) => readTransformation(conf)(spark).transform(dfA))

      Then("values should be trimmed")

      val resultList: Array[Row] = dfResult.select("trim", "trimRight", "trimLeft", "trimInvalidConfig").collect

      resultList(0).getAs[String]("trim") shouldBe testData.trim
      resultList(0).getAs[String]("trimRight") shouldBe trimRight(testData)
      resultList(0).getAs[String]("trimLeft") shouldBe trimLeft(testData)
      resultList(0).getAs[String]("trimInvalidConfig") shouldBe testData

      resultList.length shouldBe 1

      assert(
        dfResult.schema.fields.tail.forall(
          _.dataType === StringType
        )
      )

    }

    scenario("trim values with different data types") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |    transformations = [
           |      {
           |        field = "trimInt"
           |        type = "trim"
           |      },
           |      {
           |        field = "trimBoolean"
           |        type = "trim"
           |      },
           |      {
           |        field = "trimNull"
           |        type = "trim"
           |      }
           |    ]
        """.stripMargin)

      Given("column to parse")

      val columnToParse = {
        Row(10, false, None.orNull) :: Nil
      }

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("trimInt", IntegerType) ::
            StructField("trimBoolean", BooleanType) ::
            StructField("trimNull", NullType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = config.getConfigList("transformations").asScala
        .foldLeft(df)((dfA, conf) => readTransformation(conf)(spark).transform(dfA))

      Then("values should be trimmed")

      val resultList: Array[Row] = dfResult.select("trimInt", "trimBoolean", "trimNull").collect

      resultList(0).get(0) shouldBe "10"
      resultList(0).get(1) shouldBe "false"
      resultList(0).get(2) should be eq None.orNull

      resultList.length shouldBe 1

      assert(
        dfResult.schema.fields.tail.forall(
          _.dataType === StringType
        )
      )

    }
  }

  private def trimLeft(str: String) = str.dropWhile(_.isWhitespace)

  private def trimRight(str: String) = trimLeft(str.reverse).reverse

}

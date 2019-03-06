package com.datio.kirby.transformation.column

import com.datio.kirby.testUtils._
import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class IntegrityTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  private val baseConfig = ConfigFactory.parseString(
    """
      |  {
      |    type = "integrity"
      |    path = "src/test/resources/transformations/integrity"
      |    default = "00"
      |    field = "text"
      |  }
      |  """.stripMargin)

  feature("check correct parse integrity") {
    scenario("Return correct integrity parse") {
      testCase = "CD-48"
      Given("config")

      val catalogConfig = baseConfig

      Given("column to parse")

      import spark.implicits._
      val df = List(TestEntity("aa"), TestEntity("bb"), TestEntity("")).toDF

      When("apply transformations")

      val dfResult = readTransformation(catalogConfig)(spark).transform(df)

      Then("text should be change in values not included in catalog to a default value")

      val resultList: Array[Row] = dfResult.select("text").collect

      resultList.contains(Row("aa")) shouldBe true
      resultList.contains(Row("bb")) shouldBe true
      resultList.contains(Row("00")) shouldBe true
      resultList.length shouldBe 3

      result = true
    }

    scenario("Return correct integrity parser change value") {
      testCase = "CD-43"
      Given("config")

      val catalogConfig = baseConfig

      Given("column to parse")

      import spark.implicits._
      val df = List(TestEntity("aa"), TestEntity("bb"), TestEntity("cc")).toDF

      When("apply transformations")

      val dfResult = readTransformation(catalogConfig)(spark).transform(df)

      Then("text should be change in values not included in catalog to a default value")

      val resultList: Array[Row] = dfResult.select("text").collect

      resultList.contains(Row("aa")) shouldBe true
      resultList.contains(Row("bb")) shouldBe true
      resultList.contains(Row("cc")) shouldBe true
      resultList.contains(Row("00")) shouldBe false
      resultList.length shouldBe 3

      result = true
    }
  }
}
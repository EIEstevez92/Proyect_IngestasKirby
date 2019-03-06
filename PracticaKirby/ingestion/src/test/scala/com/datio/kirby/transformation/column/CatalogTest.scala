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
class CatalogTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory{

  feature("Return correct parser") {
    scenario("check correct parse with catalog") {
      Given("config")

      val catalogConfig = ConfigFactory.parseString(
        """
          |  {
          |    type = "catalog"
          |    path = "src/test/resources/transformations/dictionary.txt"
          |    field = "text"
          |  }
          |  """.stripMargin)

      Given("column to parse")

      import spark.implicits._

      val df = List(TestEntity("aa"), TestEntity("bB"), TestEntity("")).toDF

      When("apply transformations")

      val dfResult = readTransformation(catalogConfig)(spark).transform(df)

      Then("text should be change in values not included in catalog to a default value")

      val resultList: Array[Row] = dfResult.select("text").collect

      resultList.contains(Row("aa")) shouldBe false
      resultList.contains(Row("world")) shouldBe true
      resultList.length shouldBe 3
      resultList.count(_.get(0) != None.orNull) shouldBe 1
      resultList.count(_.get(0) == None.orNull) shouldBe 2

    }
  }
}
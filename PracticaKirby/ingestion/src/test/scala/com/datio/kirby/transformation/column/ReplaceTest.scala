package com.datio.kirby.transformation.column

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ReplaceTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Replace masterization") {
    scenario("replace data in column") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "replaceField"
           |        type = "replace"
           |        replace = {
           |          "change1" : "good1",
           |          "change2" : "good2"
           |        }
           |      }
        """.stripMargin)

      Given("column to parse")

      val columnToParse = Row("change1") :: Row("change2") :: Row("conserved") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("replaceField", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("value should be copied into new column")

      val resultList: Array[Row] = dfResult.select("replaceField").collect

      resultList.contains(Row("good1")) shouldBe true
      resultList.contains(Row("good2")) shouldBe true
      resultList.contains(Row("conserved")) shouldBe true

      resultList.contains(Row("change1")) shouldBe false
      resultList.contains(Row("change2")) shouldBe false

      resultList.length shouldBe 3

      assert(
        dfResult.schema.fields.tail.forall(
          _.dataType === StringType
        )
      )

    }

  }

}

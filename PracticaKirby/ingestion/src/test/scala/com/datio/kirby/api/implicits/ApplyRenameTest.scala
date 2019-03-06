package com.datio.kirby.api.implicits

import com.datio.kirby.testUtils.InitSparkSessionFeature
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class ApplyRenameTest extends InitSparkSessionFeature with ApplyRename with GivenWhenThen with Matchers {


  feature("Apply renaming to columns") {

    scenario("Apply new names to DF") {
      Given("A dataFrame with a column")
      import spark.implicits._
      val df = List(("value1", "value2")).toDF("name","name2")

      Given("A schema with rename")
      val schema = StructType(List(StructField("name", StringType, nullable = false,
        new MetadataBuilder().putString("rename", "newName").build())))

      When("Apply rename")
      val dfFormatted = df.renameColumns(schema)

      Then("Column contain new column and not old column")
      dfFormatted.columns.toSet shouldBe Set("newName", "name2")

      And("Column contain data")
      dfFormatted.collect().map(_.getAs[String]("newName")).toSet shouldBe Set("value1")

    }


    scenario("Apply new names to DF to a no existing column") {
      Given("A dataFrame with a column")
      import spark.implicits._
      val df = List(("value1", "value2")).toDF("name","name2")

      Given("A schema with rename")
      val schema = StructType(List(StructField("notExists", StringType, nullable = false,
        new MetadataBuilder().putString("rename", "newName").build())))

      When("Apply rename")
      val dfFormatted = df.renameColumns(schema)

      Then("Not rename any column")
      dfFormatted.columns.toSet shouldBe Set("name","name2")

    }
  }
}


package com.datio.kirby.input

import com.datio.kirby.config.InputFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class ParquetInputTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with InputFactory {

  feature("check if the files can be readed") {

    scenario("read the file in a valid path") {
      testCase = "CD-5"

      Given("path to file")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "parquet"
          |    paths = ["src/test/resources/configFiles/parquet/example_parquet"]
          |    schema {}
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val reader = new ParquetInput(inputConfig.getConfig("input"))
      val fields: DataFrame = reader.read(spark)

      Then("Not return any valid row")
      fields.count shouldBe 1

      fields.schema.length === Schemas.schemaParquet.length

      result = true
    }

    scenario("read two files and concatenate in a valid path") {
      testCase = "CD-46"

      Given("path to file")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "parquet"
          |    paths = [
          |      "src/test/resources/configFiles/parquet/example_parquet",
          |      "src/test/resources/configFiles/parquet/example_parquet"]
          |    schema {}
          |  }
          |  """.stripMargin)


      When("Run the reader in path")
      val reader = new ParquetInput(inputConfig.getConfig("input"))
      val fields: DataFrame = reader.read(spark)

      Then("Not return any valid row")
      fields.count shouldBe 2

      fields.schema.length === Schemas.schemaParquet.length

      result = true
    }

    scenario("Try to read a file with a not valid path") {
      testCase = "CD-7"

      Given("path to file")

      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "parquet"
          |    paths = [
          |      "src/test/resources/configFiles/parquet/bad_example_parquet"]
          |    schema {}
          |  }
          |  """.stripMargin)

      When("Try to read the file")
      val reader = new ParquetInput(inputConfig.getConfig("input"))
      val thrown: Exception = intercept[Exception] {
        reader.read(spark).count
      }

      Then("there is a null exception")
      assert(thrown.getMessage.nonEmpty)

      result = true
    }

    scenario("read a file") {

      Given("path to file")

      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "parquet"
          |    paths = [
          |      "src/test/resources/configFiles/parquet/example_parquet"]
          |    schema {}
          |  }
          |  """.stripMargin)

      When("Try to read the file")
      val reader = new ParquetInput(inputConfig.getConfig("input"))
      val fields = reader.read(spark)

      Then("Cast to correct type")

      fields
        .select("age")
        .collect()
        .forall(
          _.getAs[Int]("age").isValidInt
        ) shouldBe true

      fields.schema.length === Schemas.schemaParquet.length
    }
  }
}

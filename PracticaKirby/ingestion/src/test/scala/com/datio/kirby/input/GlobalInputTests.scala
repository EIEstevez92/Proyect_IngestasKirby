package com.datio.kirby.input

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.config.InputFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class GlobalInputTests extends InitSparkSessionFeature with GivenWhenThen with Matchers with InputFactory {

  feature("Reading an Input from configuration") {

    info("As a user")
    info("I want to create an input from a configuration")

    scenario("there is no files in path configuration") {

      Given("input configuration")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "csv"
          |    delimiter = ";"
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(inputConfig.getConfig("input")) {
        override lazy val schema: StructType = StructType(Array(StructField("cod_entalfa", StringType)))
      }

      Then("Must return an appropriate exception")
      intercept[KirbyException] {
        reader.read(spark)
      }
    }

    scenario("additional options must override default values") {
      Given("path to file")

      val inputConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/simple_example.csv"]
           |    delimiter = "|"
           |    options {
           |       header = true
           |    }
           |  }
        """.stripMargin)
      val reader = new CsvInput(inputConfig.getConfig("input")) {
        override lazy val schema: StructType = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
      }

      When("Run the reader in path")

      val names = reader.read(spark)

      Then("Header option must be overriden and read 3 registers")
      names.count shouldBe 3
    }

  }

}

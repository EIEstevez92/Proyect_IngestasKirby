package com.datio.kirby.input

import com.datio.kirby.config.InputFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class CsvInputTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with InputFactory {

  feature("check if the files can be readed") {
    scenario("read the file in a valid path") {
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/example_mae.csv"]
           |    delimiter = ";"
           |    options {
           |       mode = "DROPMALFORMED"
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(myConfig.getConfig("input")) {
        override lazy val schema = StructType(Array(StructField("cod_entalfa", StringType)))
      }
      val fields = reader.read(spark)

      Then("Not return any valid row")
      fields.count shouldBe 0

    }

    scenario("read the file in a valid path with delimiter '├'") {
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/example_mae_other_delimiter.csv"]
           |    delimiter = "├"
           |    options {
           |       mode = "DROPMALFORMED"
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(myConfig.getConfig("input")) {
        override lazy val schema = StructType(Array(StructField("cod_entalfa", StringType)))
      }
      val fields = reader.read(spark)

      Then("Not return any valid row")
      fields.count shouldBe 0

    }

    scenario("Try to read a file with a not valid path") {
      testCase = "CD-3"
      Given("path to file")

      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/badExampleFile.csv"]
           |    delimiter = ";"
           |  }
        """.stripMargin)

      When("Try to read the file")
      val reader = new CsvInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaGood
      }
      val thrown: Exception = intercept[Exception] {
        reader.read(spark).count
      }

      Then("there is a null exception")
      assert(thrown.getMessage.nonEmpty)

      result = true
    }

    scenario("read the file applying a valid schema") {
      testCase = "CD-1"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/example_mae.csv"]
           |    delimiter = ";"
           |    options {
           |       header = true
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 12

      result = true
    }

    scenario("read two files applying a valid schema") {
      testCase = "CD-2"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/example_mae.csv", "src/test/resources/configFiles/csv/example_mae.csv"]
           |    delimiter = ";"
           |    options {
           |       header = true
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 24

      result = true
    }

    scenario("read the file applying a not valid schema") {
      testCase = "CD-45"
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/example_mae.csv"]
           |    delimiter = ";"
           |    options {
           |       mode = "DROPMALFORMED"
           |    }
           |  }
        """.stripMargin)
      When("Run the reader in path")
      val reader = new CsvInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaBad
      }
      val fields = reader.read(spark)

      Then("Not Return any valid row")
      fields.count shouldBe 0

      result = true
    }

    scenario("read the file with errors in failfast mode") {
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "csv"
           |    paths = ["src/test/resources/configFiles/csv/simple_example_with_error.csv"]
           |    delimiter = "|"
           |    options {
           |        header = true
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new CsvInput(myConfig.getConfig("input")) {
        override lazy val schema = Schemas.schemaParquetNonNullable
      }
      val fields = reader.read(spark)

      Then("Intercept Runtime Exception because mode is Failfast")
      val thrown: Exception = intercept[Exception] {
        fields.count
      }
    }
  }
}

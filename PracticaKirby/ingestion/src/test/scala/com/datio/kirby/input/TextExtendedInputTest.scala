package com.datio.kirby.input

import com.datio.kirby.config.InputFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkException
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class TextExtendedInputTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with InputFactory {

  private val originName = "originName"
  private val schemaGood = StructType(Array(
    StructField("id", IntegerType, nullable = true, new MetadataBuilder().
      putString(originName, "id").build())
    , StructField("field1", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "field1").build())
    , StructField("field2", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "field2").build())
    , StructField("field3", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "field3").build())
    , StructField("field4", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "field4").build())))

  private val schemaNotNullable = StructType(Array(
    StructField("id", IntegerType, nullable = false, new MetadataBuilder().
      putString(originName, "id").build())
    , StructField("field1", StringType, nullable = false, new MetadataBuilder().
      putString(originName, "field1").build())
    , StructField("field2", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "field2").build())
    , StructField("field3", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "field3").build())
    , StructField("field4", StringType, nullable = true, new MetadataBuilder().
      putString(originName, "field4").build())))

  feature("check if the files can be readed") {
    scenario("read the file applying a valid schema") {
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "text-extended"
           |    paths = ["src/test/resources/configFiles/textextended/acceptance_input.txt"]
           |    delimiter = "'~'"
           |    delimiterRow = "#@#@#"
           |    options {
           |        dateFormat = mm-yyyy
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new TextExtendedInput(myConfig.getConfig("input")) {
        override lazy val schema = schemaGood
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 139
    }

    scenario("read the file applying a valid schema with no nullable") {
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "text-extended"
           |    paths = ["src/test/resources/configFiles/textextended/acceptance_input.txt"]
           |    delimiter = "'~'"
           |    delimiterRow = "#@#@#"
           |    options {
           |        dateFormat = mm-yyyy
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new TextExtendedInput(myConfig.getConfig("input")) {
        override lazy val schema = schemaNotNullable
      }
      val thrown = intercept[SparkException] {
        reader.read(spark).count
      }
      Then("Recover casting error")
      thrown.getCause.getMessage shouldBe "Malformed line in FAILFAST mode: '~''~'UNMAPPED'~''~'"
    }

    scenario("read the file applying a valid schema with no nullable and dropmalformed") {
      Given("path to file")
      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "text-extended"
           |    options{
           |        mode = dropmalformed
           |    }
           |    paths = ["src/test/resources/configFiles/textextended/acceptance_input.txt"]
           |    delimiter = "'~'"
           |    delimiterRow = "#@#@#"
           |    options {
           |        dateFormat = mm-yyyy
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new TextExtendedInput(myConfig.getConfig("input")) {
        override lazy val schema = schemaNotNullable
      }
      val fields = reader.read(spark)

      Then("Return all the valid rows")
      fields.count shouldBe 138
    }

    scenario("Try to read a file with a not valid path") {
      Given("path to file")

      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "text-extended"
           |    paths = ["src/test/resources/configFiles/textextended/notfound.txt"]
           |    delimiter = "'~'"
           |    delimiterRow = "#@#@#"
           |    options {
           |        dateFormat = mm-yyyy
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")
      val reader = new TextExtendedInput(myConfig.getConfig("input")) {
        override lazy val schema = schemaGood
      }
      val thrown: Exception = intercept[Exception] {
        reader.read(spark).count
      }

      Then("there is a null exception")
      assert(thrown.getMessage.nonEmpty)
    }

    scenario("delimiter is mandatory") {
      Given("path to file")

      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "text-extended"
           |    paths = ["src/test/resources/configFiles/textextended/acceptance_input.txt"]
           |    delimiterRow = "#@#@#"
           |    options {
           |        dateFormat = mm-yyyy
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")


      val thrown = intercept[SparkException] {
        val reader = new TextExtendedInput(myConfig.getConfig("input")) {
          override lazy val schema = schemaGood
        }
        val fields = reader.read(spark).count
      }

      Then("delimiter is mandatory")
      assert(thrown.getCause.getMessage === "delimiter is mandatory")
    }

    scenario("delimiterRow is mandatory") {
      Given("path to file")

      val myConfig = ConfigFactory.parseString(
        s"""
           | input {
           |    type = "text-extended"
           |    paths = ["src/test/resources/configFiles/textextended/acceptance_input.txt"]
           |    delimiter = "#@#@#"
           |    options {
           |        dateFormat = mm-yyyy
           |    }
           |  }
        """.stripMargin)

      When("Run the reader in path")


      val thrown = intercept[SparkException] {
        val reader = new TextExtendedInput(myConfig.getConfig("input")) {
          override lazy val schema = schemaGood
        }
        val fields = reader.read(spark).count
      }

      Then("delimiterRow is mandatory")
      assert(thrown.getCause.getMessage === "delimiterRow is mandatory")
    }

  }
}

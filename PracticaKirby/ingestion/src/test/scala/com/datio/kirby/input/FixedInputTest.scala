package com.datio.kirby.input

import com.datio.kirby.config.{InputFactory, SchemaReader}
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

/**
  * Created by antoniomartin on 18/10/16.
  */
@RunWith(classOf[JUnitRunner])
class FixedInputTest extends InitSparkSessionFeature with GivenWhenThen with Matchers
  with InputFactory with SchemaReader {

  def getRow(stringRow: String, colLongList: Seq[ColLong]): Row = {
    val columnArray = new Array[String](colLongList.length)

    colLongList.zipWithIndex.foreach {
      case (field, index) => columnArray(index) = stringRow.substring(field.initColumn, field.endColumn)
    }

    Row.fromSeq(columnArray)
  }

  feature("Check the diferents methods") {
    scenario("Give a Schema, get the right line Distib") {
      Given("A Standard configuration")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "fixed"
          |    paths = []
          |    schema {}
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val fixedReader = readInput(inputConfig.getConfig("input")
      )

      When("Get the distribution")
      val dist: Seq[ColLong] = fixedReader.asInstanceOf[FixedInput].getColumnDistribution(Schemas.schemaWithDistribution)

      Then("The Distribution should be fine")

      dist.head.initColumn shouldBe 0
      dist.head.endColumn shouldBe 1
      dist(1).initColumn shouldBe 1
      dist(1).endColumn shouldBe 4
      dist(2).initColumn shouldBe 4
      dist(2).endColumn shouldBe 9
      dist(3).initColumn shouldBe 9
      dist(3).endColumn shouldBe 17
      dist(4).initColumn shouldBe 17
      dist(4).endColumn shouldBe 29
      dist(5).initColumn shouldBe 29
      dist(5).endColumn shouldBe 50
      dist(6).initColumn shouldBe 50
      dist(6).endColumn shouldBe 52
      dist(7).initColumn shouldBe 52
      dist(7).endColumn shouldBe 56
      dist(8).initColumn shouldBe 56
      dist(8).endColumn shouldBe 62
    }

    scenario("Give a Schema and a line, get the right RDD") {
      Given("A Standard configuration")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "fixed"
          |    paths = []
          |    schema {}
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val fixedReader = readInput(inputConfig.getConfig("input")
      )

      When("Get the line distribution")
      val dist: Seq[ColLong] = fixedReader.asInstanceOf[FixedInput].getColumnDistribution(Schemas.schemaWithDistribution)
      val line = "AENGAEIOUTESTTESTTWELVE CHARSTWENTY-ONE CHARACTERSBBCCCCDDDDDD"
      val processRaw: Row = getRow(line, dist)

      Then("The Distribution should be fine")
      processRaw.get(0) shouldBe "A"
      processRaw.get(1) shouldBe "ENG"
      processRaw.get(2) shouldBe "AEIOU"
      processRaw.get(3) shouldBe "TESTTEST"
      processRaw.get(4) shouldBe "TWELVE CHARS"
      processRaw.get(5) shouldBe "TWENTY-ONE CHARACTERS"
      processRaw.get(6) shouldBe "BB"
      processRaw.get(7) shouldBe "CCCC"
      processRaw.get(8) shouldBe "DDDDDD"
    }

    scenario("Give a Schema and a line with decimal, get the right RDD") {
      Given("A Standard configuration")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "fixed"
          |    paths = []
          |    schema {}
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val fixedReader = readInput(inputConfig.getConfig("input")
      )

      When("Get the line distribution")
      val dist: Seq[ColLong] = fixedReader.asInstanceOf[FixedInput].getColumnDistribution(Schemas.schemaWithDecimal)
      val line = "ES34124589-1234,5687"
      val processRaw: Row = getRow(line, dist)

      Then("The Distribution should be fine")
      processRaw.get(0) shouldBe "ES34"
      processRaw.get(1) shouldBe "124589"
      processRaw.get(2) shouldBe "-1234,5687"

    }
  }

  feature("check if the files can be readed") {
    scenario("read the file in a valid path") {
      testCase = "CD-4"
      Given("path to file")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "fixed"
          |    paths = [
          |      "src/test/resources/configFiles/fixed/example_mae.txt"
          |    ]
          |    delimiter = ";"
          |    schema {
          |      type = "current"
          |      path = "src/test/resources/configFiles/schemasAvro/t_kdat_mae.json"
          |      delimiter = ";"
          |    }
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val reader = readInput(inputConfig.getConfig("input"))
      val fields: DataFrame = reader.read(spark)

      Then("Return ten rows")
      fields.count shouldBe 10
      result = true
    }

    scenario("read two files in a valid path") {
      testCase = "CD-47"
      Given("path to file")
      val inputConfig = ConfigFactory.parseString(
        """
          |  input {
          |    type = "fixed"
          |    paths = [
          |      "src/test/resources/configFiles/fixed/example_mae.txt",
          |      "src/test/resources/configFiles/fixed/example_mae.txt"
          |    ]
          |    delimiter = ";"
          |    schema {
          |      type = "current"
          |      path = "src/test/resources/configFiles/schemasAvro/t_kdat_mae.json"
          |      delimiter = ";"
          |    }
          |  }
          |  """.stripMargin)

      When("Run the reader in path")
      val reader = readInput(inputConfig.getConfig("input"))
      val fields: DataFrame = reader.read(spark)

      Then("Return twenty rows")
      fields.count shouldBe 20

      result = true
    }
  }

  scenario("read a file with lines malformed and mode FAILFAST") {
    testCase = "CD-47"
    Given("path to file")
    val inputConfig = ConfigFactory.parseString(
      """
        |  input {
        |    type = "fixed"
        |    paths = [
        |      "src/test/resources/configFiles/fixed/example_malformed.txt"
        |    ]
        |    options {
        |      mode = "FAILFAST"
        |    }
        |    delimiter = ";"
        |    schema {
        |      type = "current"
        |      path = "src/test/resources/configFiles/schemasAvro/t_kdat_mae.json"
        |      delimiter = ";"
        |    }
        |  }
        |  """.stripMargin)

    When("Run the reader in path")
    val reader = readInput(inputConfig.getConfig("input"))
    val caught =
      intercept[SparkException] {
        reader.read(spark).count
      }
    caught.getCause.getMessage shouldBe "Malformed line in FAILFAST mode: ESCCCCESdfgdfgdfgdfgdfgdfgd"
    Then("Return ten rows")

  }


  scenario("read a file with lines malformed and mode DROPMALFORMED") {
    testCase = "CD-47"
    Given("path to file")
    val inputConfig = ConfigFactory.parseString(
      """
        |  input {
        |    type = "fixed"
        |    paths = [
        |      "src/test/resources/configFiles/fixed/example_malformed.txt"
        |    ]
        |    options {
        |      mode = "DROPMALFORMED"
        |    }
        |    delimiter = ";"
        |    schema {
        |      type = "current"
        |      path = "src/test/resources/configFiles/schemasAvro/t_kdat_mae.json"
        |      delimiter = ";"
        |    }
        |  }
        |  """.stripMargin)

    When("Run the reader in path")
    val reader = readInput(inputConfig.getConfig("input"))
    val fields: DataFrame = reader.read(spark)

    Then("Return ten rows")
    fields.count shouldBe 10

    result = true
  }

}

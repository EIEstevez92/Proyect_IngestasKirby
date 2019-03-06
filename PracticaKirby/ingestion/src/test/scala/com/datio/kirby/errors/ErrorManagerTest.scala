package com.datio.kirby.errors

import com.datio.kirby.config.validator.ValidatorException
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ErrorManagerTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val CONFIG_FILE = "config file"
  val RUNNING_APP = "Run the app"
  val WELL_FORMED_FILE = "File is well-formed"
  val BAD_FORMED_FILE = "File is bad-formed"
  val THERE_IS_NO_ERRORS = "There is no errors"
  val input = "input"
  val output = "output"
  val schema = "schema"

  feature("Errors in input config") {

    scenario("Input configuration without paths is not allowed") {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | input {
          |    type = "csv"
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasAValidInputConfig(config.getConfig(input))

      Then("input configuration without paths is not allowed")
      assert(Option(errorsMap(CONFIG_INPUT_MANDATORY_ERROR.code)).isDefined)
      assert(errorsMap(CONFIG_INPUT_MANDATORY_ERROR.code) == CONFIG_INPUT_MANDATORY_ERROR.messageToFormattedString("paths"))
    }

    scenario("Input configuration without type is not allowed") {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | input {
          |    paths = []
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasAValidInputConfig(config.getConfig(input))

      Then("input configuration without type is permitted")
      assert(Option(errorsMap(CONFIG_INPUT_MANDATORY_ERROR.code)).isDefined)
      assert(errorsMap(CONFIG_INPUT_MANDATORY_ERROR.code) == CONFIG_INPUT_MANDATORY_ERROR.messageToFormattedString("type"))
    }

    scenario(WELL_FORMED_FILE) {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | input {
          |    paths = []
          |    type = "csv"
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasAValidInputConfig(config.getConfig(input))

      Then(THERE_IS_NO_ERRORS)
      assert(errorsMap.isEmpty)
    }

  }

  feature("Errors in output config") {

    scenario("Output configuration without type is not allowed") {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | output {
          |    path = ""
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasAValidOutputConfig(config.getConfig(output))

      Then("output type does not exist")
      assert(Option(errorsMap(CONFIG_OUTPUT_MANDATORY_ERROR.code)).isDefined)
      assert(errorsMap(CONFIG_OUTPUT_MANDATORY_ERROR.code) == CONFIG_OUTPUT_MANDATORY_ERROR.messageToFormattedString("type"))
    }

    scenario("Output configuration without path is not allowed)") {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | output {
          |    type = ""
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasAValidOutputConfig(config.getConfig(output))

      Then("output path does not exist")
      assert(Option(errorsMap(CONFIG_OUTPUT_MANDATORY_ERROR.code)).isDefined)
      assert(errorsMap(CONFIG_OUTPUT_MANDATORY_ERROR.code) == CONFIG_OUTPUT_MANDATORY_ERROR.messageToFormattedString("path"))
    }

    scenario(WELL_FORMED_FILE) {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | output {
          |    path = ""
          |    type = ""
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errors = ErrorManager.hasAValidOutputConfig(config.getConfig(output))

      Then(THERE_IS_NO_ERRORS)
      assert(errors.isEmpty)
    }

  }

  feature("Errors in schema config") {

    scenario("File without schema is not allowed)") {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | output {
          |    type = ""
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasASchemaDefined(config)

      Then("output var path does not exist")
      assert(errorsMap(CONFIG_IO_SCHEMA_ERROR.code) == CONFIG_IO_SCHEMA_ERROR.messageToFormattedString())
    }

    scenario(WELL_FORMED_FILE) {

      Given(CONFIG_FILE)
      val config: Config = ConfigFactory.parseString(
        """
          | output{
          |   schema {
          |   }
          | }
          | input{
          |   schema {
          |   }
          | }
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasASchemaDefined(config)

      Then(THERE_IS_NO_ERRORS)
      assert(errorsMap.isEmpty)
    }

  }

  feature("Errors in the transformations configuration") {

    scenario("type does not exist in one masterization") {
      Given(CONFIG_FILE)

      val config = ConfigFactory.parseString(
        """        transformations = [
          |        {
          |          type = "literal"
          |          field = "allValuesInCatalog"
          |        },
          |        {
          |          type = "catalog"
          |          field = "allValuesInCatalog"
          |        },
          |        {
          |          typa = "literal"
          |          field = "allValuesInCatalog"
          |        },
          |        {
          |          type = "catalog"
          |          field = "allValuesInCatalog"
          |        }
          |      ]
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasValidTransformations(config)

      Then(WELL_FORMED_FILE)
      assert(errorsMap(CONFIG_TRANSFORMATION_MANDATORY_ERROR.code) == CONFIG_TRANSFORMATION_MANDATORY_ERROR.messageToFormattedString("type"))
    }

    scenario("Configuration is well-formed") {
      Given(CONFIG_FILE)

      val config = ConfigFactory.parseString(
        """        transformations = [
          |        {
          |          type = "literal"
          |          field = "allValuesInCatalog"
          |        },
          |        {
          |          type = "catalog"
          |          field = "allValuesInCatalog"
          |        },
          |        {
          |          type = "literal"
          |          field = "allValuesInCatalog"
          |        },
          |        {
          |          type = "cleanNulls"
          |        }
          |      ]
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.hasValidTransformations(config)

      Then(WELL_FORMED_FILE)
      assert(errorsMap.isEmpty)
    }

  }

  feature("checks all functions") {

    scenario(WELL_FORMED_FILE) {
      Given(CONFIG_FILE)

      val config = ConfigFactory.parseString(
        """kirby {
          |
          |  input {
          |    type = ""
          |    paths = [
          |    ]
          |    schema {
          |    }
          |  }
          |
          |  output {
          |    type = ""
          |    path = ""
          |    schema {
          |    }
          |  }
          |
          |  transformations = [
          |    {
          |      type = "literal"
          |      field = "tokendefault"
          |    },
          |    {
          |      type = "token"
          |      field = "tokendefault"
          |    }
          |    {
          |      type = "literal"
          |      field = "tokennif"
          |    },
          |    {
          |      type = "token"
          |      field = "tokennif"
          |    },
          |    {
          |      type : "cleannulls"
          |    },
          |    {
          |      type : "dropDuplicates"
          |    }
          |  ]
          |}
        """.stripMargin)

      When(RUNNING_APP)
      val errorsMap = ErrorManager.checkErrorConfiguration(config.getConfig("kirby"))

      Then(WELL_FORMED_FILE)
      assert(errorsMap.isEmpty)
    }

    scenario(BAD_FORMED_FILE) {
      Given(CONFIG_FILE)

      val config = ConfigFactory.parseString(
        """kirby {
          |
          |  input {
          |    paths = [
          |    ]
          |    schema {
          |    }
          |  }
          |
          |  output {
          |    type = ""
          |    schema {
          |    }
          |  }
          |
          |  transformations = [
          |    {
          |      type = "literal"
          |      field = "tokendefault"
          |    },
          |    {
          |      type = "token"
          |      field = "tokendefault"
          |    }
          |    {
          |      type = "literal"
          |      field = "tokennif"
          |    },
          |    {
          |      pipe = "token"
          |      field = "tokennif"
          |    },
          |    {
          |      type : "cleannulls"
          |    },
          |    {
          |      type : "dropDuplicates"
          |    }
          |  ]
          |}
        """.stripMargin)

      When(RUNNING_APP)
      Then("an exception is thrown")
      intercept[ValidatorException] {
        ErrorManager.checkErrorConfiguration(config.getConfig("kirby"))
      }
    }
  }
}
package com.datio.kirby.config.validator

import com.datio.kirby.config.validator.ConfigValidator.schema
import com.github.fge.jsonschema.core.report.LogLevel
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class ConfigValidatorTest extends FeatureSpec
  with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private val confErrorLocation = ConfigFactory.parseString(
    """CD {
      |  input = {
      |    path = "path"
      |  }
      |}
      |""".stripMargin)


  private val confOKLocation = ConfigFactory.parseString(
    """kirby {
      |  input {
      |    type = "fixed"
      |    paths = [
      |      "hdfs://hadoop:9000/tests/flow/fixed/fixed.txt"
      |    ]
      |    schema = {
      |      preserveLegacyName = true
      |      path = "hdfs://hadoop:9000/tests/flow/schema/avro/schema_fixed.json"
      |      delimiter = ";"
      |    }
      |  }
      |  output {
      |    type = "avro"
      |    path = "hdfs://hadoop:9000/tests/flow/result/avro/fixed.avro"
      |    mode = "overwrite"
      |    schema = {
      |      path = "hdfs://hadoop:9000/tests/flow/schema/avro/schema_fixed.json"
      |    }
      |  }
      |}
      |sparkMetrics {
      |  listeners = ["default"]
      |  output {
      |    type = "console"
      |  }
      |}
      |""".stripMargin)

  val SCENARIO_ERROR = "ERROR"
  val SCENARIO_OK = "OK"

  feature("validate") {
    scenario(SCENARIO_ERROR) {
      try {
        val config = confErrorLocation
        val messages = ConfigValidator.validate(config, schema, LogLevel.ERROR)
        assert(messages.nonEmpty)
      } catch {
        case _: Throwable => assert(false)
      }
    }

    scenario(SCENARIO_OK) {
      try {
        val json = confOKLocation
        val logLevel = LogLevel.ERROR
        assert(ConfigValidator.validate(json, schema, logLevel).isEmpty)
      } catch {
        case _: Throwable => assert(false)
      }
    }
  }

  feature("validate with Config") {
    scenario(SCENARIO_ERROR) {
      try {
        val config = confErrorLocation
        val messages = ConfigValidator.validate(config, schema, LogLevel.ERROR)
        assert(messages.nonEmpty)
      } catch {
        case _: Throwable => assert(false)
      }
    }

    scenario(SCENARIO_OK) {
      try {
        val json = confOKLocation
        val logLevel = LogLevel.ERROR
        assert(ConfigValidator.validate(json, schema, logLevel).isEmpty)
      } catch {
        case _: Throwable => assert(false)
      }
    }
  }

}
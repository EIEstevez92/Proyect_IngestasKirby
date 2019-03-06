package com.datio.kirby.config.validator

import com.datio.kirby.constants.ConfigConstants.APPLICATION_CONF_SCHEMA
import com.datio.kirby.errors.CONFIG_FORMAT_EXCEPTION
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jackson.JsonLoader
import com.github.fge.jsonschema.core.report.{LogLevel, ProcessingMessage}
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging

object ConfigValidator extends ConfigValidator {

  def apply(config: Config): Unit = {
    val messages = ConfigValidator.validate(config, schema, LogLevel.ERROR)
    checkMessages(messages)
  }

  def checkMessages(messages: List[ProcessingMessage]): Unit = {
    if (messages.nonEmpty) {
      logger.warn(messages.mkString)
      throw new ValidatorException(CONFIG_FORMAT_EXCEPTION, messages.mkString)
    } else {
      logger.info(s"${this.getClass.getSimpleName} check success")
    }
  }

  def schema: InputReadable = {
    InputResource(APPLICATION_CONF_SCHEMA)
  }

}

/** It's used for validate configs against schemas.
  *
  */
class ConfigValidator extends LazyLogging {

  def validate[T <: InputReadable, K <: InputReadable]
  (configFile: T, schemaFile: K, logLevel: LogLevel): List[ProcessingMessage] = {
    val config = Converter.asConfig(configFile.content)
    validate(config, schemaFile, logLevel)
  }

  def validate[K <: InputReadable](config: Config, schemaFile: K,
                                   logLevel: LogLevel): List[ProcessingMessage] = {
    val json = Converter.asJson(config)
    val schema = Converter.asSchema(schemaFile.content)
    validateInner(json, schema, logLevel)
  }

  protected def validateInner(json: JsonNode,
                              schema: JsonSchema, logLevel: LogLevel): List[ProcessingMessage] = {
    JsonValidator(schema).generateReport(json).messages(logLevel)
  }

  object Converter extends Converter

  /** This class is for ConfigValidator, parse available validation input types.
    *
    * This converter take an object type and parse to available types of config
    *
    * Example: Converter.asJson(config)
    * We receive a Config input and parse to JsonNode.
    * This JsonNode can be validate against any JsonSchema.
    */
  class Converter {

    def asConfig(content: String): Config = {
      ConfigFactory.parseString(content)
    }

    def asJson(config: Config): JsonNode = {
      val json = config.root().render(ConfigRenderOptions.concise())
      asJson(json)
    }

    def asJson(content: String): JsonNode = {
      JsonLoader.fromString(content)
    }

    def asSchema(content: String): JsonSchema = {
      val jsonNode = asJson(content)
      val factory: JsonSchemaFactory = JsonSchemaFactory.byDefault()
      factory.getJsonSchema(jsonNode)
    }

  }

}

package com.datio.kirby.errors

import com.datio.kirby.config.validator.ValidatorException
import com.datio.kirby.constants.ConfigConstants._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters.asScalaBufferConverter

object ErrorManager extends LazyLogging {

  val typeField = "type"

  /**
    * Check logic of configuration file. Check structure and mandatory fields.
    * @param config Configuration object.
    * @return
    */
  def checkErrorConfiguration(config: Config): Map[Int, String] = {
    logger.info("Checking Kirby configuration file...")

    val validationMap = hasAValidInputConfig(config.getConfig(INPUT_CONF_KEY)) ++
      hasAValidOutputConfig(config.getConfig(OUTPUT_CONF_KEY)) ++
      hasASchemaDefined(config) ++
      hasValidTransformations(config)

    if(validationMap.nonEmpty){
      val messages = for(key <- validationMap.keys) yield validationMap(key)
      throw new ValidatorException(CONFIG_CONTENT_EXCEPTION, messages.mkString("\n"))
    }

    validationMap
  }

  /**
    * Check if input has 'paths' and 'type' fields.
    *
    * @param config Config object.
    * @return a map with error number in key and description in value.
    */
  private[errors] def hasAValidInputConfig(config: Config): Map[Int, String] = {
    (for (fieldToCheck <- Seq(typeField, "paths")) yield {
      if (!config.hasPath(fieldToCheck)) {
        Option(CONFIG_INPUT_MANDATORY_ERROR.code -> CONFIG_INPUT_MANDATORY_ERROR.messageToFormattedString(fieldToCheck))
      } else {
        None
      }
    }).flatten.toMap
  }

  /**
    * Check if output configuration has 'path' and 'type' fields.
    *
    * @param config Config object.
    * @return a map with error number in key and description in value.
    */
  private[errors] def hasAValidOutputConfig(config: Config): Map[Int, String] = {
    (for (fieldToCheck <- Seq(typeField, "path")) yield {
      if (!config.hasPath(fieldToCheck)) {
        Option(CONFIG_OUTPUT_MANDATORY_ERROR.code -> CONFIG_OUTPUT_MANDATORY_ERROR.messageToFormattedString(fieldToCheck))
      } else {
        None
      }
    }).flatten.toMap
  }

  /**
    * Check if config has schema defined in input and output.
    *
    * @param config Config object.
    * @return a map with error number in key and description in value.
    */
  private[errors] def hasASchemaDefined(config: Config): Map[Int, String] = {
    if (!config.hasPath(s"$OUTPUT_CONF_KEY.$SCHEMA_CONSTANT")) {
      Map(CONFIG_IO_SCHEMA_ERROR.code -> CONFIG_IO_SCHEMA_ERROR.message)
    } else {
      Map[Int, String]()
    }
  }

  /**
    * Check if transformations has type defined.
    *
    * @param config Configuration object.
    * @return a map with error number in key and description in value.
    */
  private[errors] def hasValidTransformations(config: Config): Map[Int, String] = {
    if (config.hasPath(TRANSFORMATION_CONF_KEY)) {
      val confTransformations = config.getConfigList(TRANSFORMATION_CONF_KEY).asScala
      (for (transformation <- confTransformations) yield {
        if (!transformation.hasPath(typeField)) {
          Option(CONFIG_TRANSFORMATION_MANDATORY_ERROR.code -> CONFIG_TRANSFORMATION_MANDATORY_ERROR.messageToFormattedString(typeField))
        } else {
          None
        }
      }).flatten.toMap
    } else {
      Map[Int, String]()
    }
  }
}

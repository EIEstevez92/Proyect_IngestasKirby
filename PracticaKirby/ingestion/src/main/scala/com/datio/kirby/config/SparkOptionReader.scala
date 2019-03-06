package com.datio.kirby.config

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.{SPARK_OPTION_WRONG_TYPE, SPARK_OPTION_READ_ERROR}
import com.typesafe.config.Config
import com.typesafe.config.ConfigException.{Missing, WrongType}

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

trait SparkOptionReader {

  /**
    * Read all spark options listed from config and validate then
    *
    * @param config config to retrieve options
    * @return Map with the options retrieved Map([sparkConfigKey], [value])
    */
  def readSparkOptions(config: Config, sparkOptions: Seq[SparkOption[_]]): Map[String, Any] = (
    for {
      option <- sparkOptions
      confValue <- option.getValue(config)
    } yield option.sparkKey -> confValue).toMap

  /**
    * Read a option from config and validate then
    *
    * @param configPath path from config to retrieve options
    * @param config config
    * @return Value retrieved or None
    */
  def readOption[T](configPath: String, config: Config, getConfig: (java.lang.String) => T)(implicit tag: TypeTag[T]): Option[T] = {
    Try(getConfig(configPath)) match {
      case Success(a) => Some(a)
      case Failure(_: Missing) => None
      case Failure(_: WrongType) => throw new KirbyException(SPARK_OPTION_WRONG_TYPE, configPath)
      case Failure(_) => throw new KirbyException(SPARK_OPTION_READ_ERROR, configPath)
    }
  }
}

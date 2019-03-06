package com.datio.kirby.config

import com.datio.kirby.api.Input
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.INPUT_CLASS_NOT_IMPLEMENTED
import com.typesafe.config.Config

trait InputFactory extends ConfigHelper {

  private lazy val inputs = getImplementations[Input]

  /**
    * Create an appropriate Input implementation after reading
    * the configuration.
    *
    * @param config input configuration.
    * @return the input.
    */
  def readInput(config: Config): Input = {
    val inputClass = config.getString("type").toLowerCase() match {
      case "custom" => Class.forName(config.getString("class"))
      case other: String => inputs.get(other) match {
        case Some(clazz) => clazz
        case None => throw new KirbyException(INPUT_CLASS_NOT_IMPLEMENTED, other)
      }
    }
    inputClass
      .getConstructor(classOf[Config])
      .newInstance(config)
      .asInstanceOf[Input]
  }
}




package com.datio.kirby.config

import com.datio.kirby.api.Output
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.OUTPUT_CLASS_NOT_IMPLEMENTED
import com.typesafe.config.Config

trait OutputFactory extends ConfigHelper {

  private lazy val outputs = getImplementations[Output]

  /**
    * Create an appropriate Output implementation after reading
    * the configuration.
    *
    * @param config output configuration.
    * @return the output.
    */
  def readOutput(config: Config): Output = {
    val outputClass = config.getString("type").toLowerCase() match {
      case "custom" => Class.forName(config.getString("class"))
      case other: String => outputs.get(other) match {
        case Some(clazz) => clazz
        case None => throw new KirbyException(OUTPUT_CLASS_NOT_IMPLEMENTED, other)
      }
    }
    outputClass
      .getConstructor(classOf[Config])
      .newInstance(config)
      .asInstanceOf[Output]
  }
}

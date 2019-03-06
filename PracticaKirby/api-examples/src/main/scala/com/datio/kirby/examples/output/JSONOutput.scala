package com.datio.kirby.examples.output

import com.datio.kirby.api.Output
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrameWriter, Row}

/**
  * Output api trait.
  * To implement outputs, extends this trait.
  */
class JSONOutput(val config : Config) extends Output {

  protected def writeDF (dfw : DataFrameWriter[Row]) : Unit = {
    dfw.json(path)
  }


}

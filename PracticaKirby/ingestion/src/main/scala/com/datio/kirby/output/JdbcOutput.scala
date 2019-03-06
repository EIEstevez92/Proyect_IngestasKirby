package com.datio.kirby.output

import java.net.MalformedURLException

import com.datio.kirby.api.Output
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrameWriter, Row}

import scala.util.matching.Regex

@configurable("jdbc")
class JdbcOutput(val config: Config) extends Output {

  override lazy val path: String = ""
  val url: String = config.getString("url")
  val pattern: Regex = "jdbc:[\\w]*:".r
  if (pattern.findFirstIn(url).isEmpty) {
    throw new MalformedURLException("JDBC url format is incorrect")
  }
  val table: String = config.getString("table")
  val props = new java.util.Properties
  props.setProperty("user", config.getString("user"))
  props.setProperty("password", config.getString("password"))
  props.setProperty("driver", config.getString("driver"))

  override protected def writeDF(dfw: DataFrameWriter[Row]): Unit = {

    logger.info("Output: JdbcOutput writer")

    dfw.jdbc(url = url, table = table, connectionProperties = props)
  }

  override protected def checkOverwriteMode(modeConfig: String): Unit = {}

  override protected def applyReprocessMode(modeConfig: String): String = modeConfig
}

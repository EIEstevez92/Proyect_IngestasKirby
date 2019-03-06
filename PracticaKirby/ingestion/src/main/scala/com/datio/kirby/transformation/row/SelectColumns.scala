package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

@configurable("selectcolumns")
class SelectColumns(val config: Config) extends RowTransformation {

  private val columnsToSelect  = config.getStringList("columnsToSelect").asScala.map(_.toColumnName)

  override def transform(df: DataFrame): DataFrame = {

    logger.info(s"SelectColumns: selected columns: ${columnsToSelect.mkString(", ")}")
    logger.debug(s"SelectColumns: original columns: ${df.columns.mkString(", ")}")

    if (columnsToSelect.isEmpty) {
      throw SelectColumnsException("at least one column must be selected")
    } else {
      df.select(columnsToSelect.map(df(_)): _*)
    }
  }
}


case class SelectColumnsException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)

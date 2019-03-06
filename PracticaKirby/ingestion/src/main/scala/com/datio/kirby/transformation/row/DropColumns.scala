package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

/**
  * This class removes columns by config.
  *
  * @param config drop columns.
  */
@configurable("dropcolumns")
class DropColumns(val config: Config) extends RowTransformation {

  private val columnsToDrop = config.getStringList("columnsToDrop").asScala.map(_.toColumnName)

  override def transform(df: DataFrame): DataFrame = {

    logger.info(s"DropColumns: drop columns: ${columnsToDrop.mkString(",")}")
    logger.debug(s"DropColumns: original columns: ${df.columns.mkString(",")}")

    columnsToDrop.foldLeft(df) {
      (dfDroppedColumn, nextDropColumnName) =>
        dfDroppedColumn.drop(nextDropColumnName)
    }

  }
}

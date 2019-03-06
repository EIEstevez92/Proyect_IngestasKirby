package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

/**
  * This class rename columns by config.
  *
  * @param config rename columns.
  */
@configurable("renamecolumns")
class RenameColumns(val config: Config) extends RowTransformation {

  private lazy val columnsRename = config.getConfig("columnsToRename")

  private lazy val keysToChange: Seq[String] = columnsRename.entrySet().asScala.toList.map(_.getKey).map(_.toColumnName)

  override def transform(df: DataFrame): DataFrame = {

    logger.info(s"RenameColumnsCheck: renames columns: ${keysToChange.mkString(",")}")
    logger.debug(s"RenameColumnsCheck: original columns: ${df.columns.mkString(",")}")

    val catalogKeysToChange: Map[String, String] =
      keysToChange
        .foldLeft(
          Map[String, String]()
        )(
          (totalMap, key) =>
            totalMap + (key -> columnsRename.getString(key))
        )

    catalogKeysToChange.foldLeft(df) {
      (dfRenamedColumn, nextRenameColumnName) =>
        dfRenamedColumn.withColumnRenamed(nextRenameColumnName._1, nextRenameColumnName._2)
    }

  }
}

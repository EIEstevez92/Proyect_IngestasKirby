package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.dataframeAddUtils._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


/**
  * This class removes duplicates from primary keys. If duplicates don't find, all columns will remove
  *
  * @param config check drop duplicates.
  */
@configurable("dropduplicates")
class DropDuplicates(val config: Config) extends RowTransformation {

  override def transform(df: DataFrame): DataFrame = {

    val columnsToCheck: Seq[String] = Try(config.getStringList("primaryKey")) match {
      case Success(l) if l.isEmpty => df.columns
      case Failure(_: ConfigException.Missing) => df.columns
      case Success(l) => l.asScala
      case Failure(e) => throw e
    }

    logger.info(s"DropDuplicates: Checking drop duplicates with following " +
      s"primary keys: ${columnsToCheck.mkString(",")}")

    df.dropDuplicatesAndPreserveMetadata(columnsToCheck)
  }
}

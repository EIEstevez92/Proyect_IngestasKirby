package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * This class removes nulls from primary keys. If primary keys don't find, all columns will remove
  *
  * @param config transformation clean nulls.
  */
@configurable("cleannulls")
class CleanNulls(val config: Config) extends RowTransformation {

  override def transform(df: DataFrame): DataFrame = {

    val columnsToCheck: Seq[String] = Try(config.getStringList("primaryKey")) match {
      case Success(l) => l.asScala
      case Failure(_: ConfigException.Missing) => df.columns
      case Failure(e) => throw e
    }

    logger.info("CleanNulls: checking clean nulls with following " +
      s"primary keys $columnsToCheck}")

    val dfCleaned = df.na.drop(columnsToCheck)

    dfCleaned
  }
}

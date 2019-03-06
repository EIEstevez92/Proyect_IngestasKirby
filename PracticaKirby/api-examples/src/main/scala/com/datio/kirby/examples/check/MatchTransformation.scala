package com.datio.kirby.examples.check

import com.datio.kirby.api.RowTransformation
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
  * Matcher check, that drop rows whose dont match field with matcher string.
  * @param config match checker config.
  */
class MatchTransformation(val config : Config) extends RowTransformation {

  val field = config.getString("field")
  val matcherList = config.getString("matcherList")

  override def transform(df: DataFrame): DataFrame = {

    logger.info("MatchChecker: checking match Strings")

    val dfResult = df
        .filter(df(field).isin(matcherList.split(",") : _*))

    dfResult
  }

}

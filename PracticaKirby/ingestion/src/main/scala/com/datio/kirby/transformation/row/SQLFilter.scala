package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

@configurable("sqlfilter")
class SQLFilter(val config: Config) extends RowTransformation {

  private val filter = config.getString("filter")

  override def transform(df: DataFrame): DataFrame = {
    df.filter(filter)
  }
}

package com.datio.kirby.transformation.row

import java.util.Collections

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.regexp_extract


import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Try

/**
  * Return or overwrite a column after applying a regex pattern in a origin column
  *
  * @param config config for RegexColumn masterization.
  */
@configurable("regexcolumn")
class RegexColumn(val config: Config)(implicit val spark: SparkSession) extends RowTransformation{


  lazy val columnToRegex: String = config.getString("columnToRegex")
  lazy val regexPattern: String = config.getString("regexPattern")
  lazy val regexList: Seq[(Int, String)] = {
    Try(config.getConfigList("regex")).getOrElse(Collections.emptyList[Config]())
  }.asScala.map { rgx =>
    (Try(rgx.getInt("regexGroup")).getOrElse(0),
      rgx.getString("field"))
  }

    override def transform(df: DataFrame): DataFrame = {

      regexList.foldLeft(df) {
        case (df, (regexGroup, field))
        => df.withColumn(field, regexp_extract(df.col(columnToRegex), regexPattern, regexGroup))

      }
    }

}
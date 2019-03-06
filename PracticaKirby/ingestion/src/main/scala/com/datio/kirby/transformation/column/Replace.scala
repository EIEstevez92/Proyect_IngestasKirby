package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._

/**
  * Change field value for replace value if exist
  *
  * @param config config for Replace masterization.
  */
@configurable("replace")
class Replace(val config: Config) extends ColumnTransformation {

  private lazy val replaceConfig = config.getConfig("replace")

  private lazy val keysToChange: Seq[String] = replaceConfig.entrySet().asScala.toList.map(_.getKey)

  /**
    * Custom transform to add udf.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  override def transform(col: Column): Column = {

    // workaround to solve serializable task errors when count and log errors inside udf.
    val logSerializable = logger
    val catalogKeysToChange: Map[String, String] =
      keysToChange
        .foldLeft(
          Map[String, String]()
        )(
          (totalMap, key) =>
            totalMap + (key -> replaceConfig.getString(key))
        )

    logSerializable.info("transform {} for column: {}, with map: {}",
      transformName, columnName, keysToChange.mkString(","))

    val method: String => String = key => {

      if (catalogKeysToChange.contains(key)) {
        catalogKeysToChange.get(key).head
      } else {
        key
      }

    }

    val udfToApply = udf(method)
    udfToApply(col)
  }

}

package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

/**
  * Checks key in dictionary. If key is not present, it will put default value.
  *
  * @param config config for integrity masterization.
  * @param spark  spark session object.
  */
@configurable("integrity")
class Integrity(val config: Config)(implicit val spark: SparkSession) extends ColumnTransformation {

  private lazy val path = config.getString("path")
  lazy val catalog: Seq[String] = readIntegrityCatalog(spark)
  lazy val default: String = config.getString("default")

  /**
    * Custom transform to add udf.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  override def transform(col: Column): Column = {

    // workaround to solve serializable task errors when count and log errors inside udf.
    val logSerializable = logger
    val catalogSerializable = catalog
    val defaultSerializable = default
    val pathSerializable = path

    logSerializable.info("transform {} for column: {}", transformName, columnName)

    val method: String => String = key => {
      if (catalogSerializable.contains(key)) {
        key.toString
      } else {
        logSerializable.debug("Fail in transform Integrity -> {} has not found in catalog: {}",
          key, pathSerializable)
        defaultSerializable
      }

    }

    val udfToApply = udf(method)

    udfToApply(col)

  }

  def readIntegrityCatalog(spark: SparkSession): Seq[String] = {
    spark.sparkContext.textFile(path).collect.toSeq
  }

}

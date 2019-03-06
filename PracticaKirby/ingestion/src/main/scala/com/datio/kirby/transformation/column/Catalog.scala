package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession}

/**
  * Change field value (key) for dictionary value
  *
  * @param config config for catalog masterization.
  * @param spark  spark session.
  */
@configurable("catalog")
class Catalog(val config: Config)(implicit val spark: SparkSession) extends ColumnTransformation {

  lazy val path: String = config.getString("path")

  lazy val map: Map[String, String] = readDictionary(spark, path)

  /**
    * Custom transform to add udf.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  override def transform(col: Column): Column = {

    // workaround to solve serializable task errors when count and log errors inside udf.
    val logSerializable = logger
    val mapSerializable = map
    val pathSerializable = path

    logSerializable.info("transform {} for column: {}", transformName, columnName)

    val method: Any => Option[String] = key => {

      if (mapSerializable.contains(key.asInstanceOf[String])) {
        mapSerializable.get(key.asInstanceOf[String])
      } else {
        logSerializable.debug(s"fail in Catalog -> $pathSerializable has not contains key: $key")
        None
      }

    }

    val udfToApply = udf(method)

    udfToApply(col)

  }

  def readDictionary(spark: SparkSession, path: String): Map[String, String] = {

    logger.debug("Masterization: Read catalog from path : {}", path)

    import spark.implicits._

    spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true").load(path)
      .map(r => (r(0).toString, r(1).toString))
      .collect()
      .toMap
  }

}

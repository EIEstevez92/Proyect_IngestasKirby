package com.datio.kirby.api.implicits

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.util.{Success, Try}

/** Apply rename dataFrame util to rename columns
  */
trait ApplyRename extends LazyLogging {

  /** implicit class used to rename dataFrame column types
    *
    * @param df input dataFrame
    */
  implicit class ApplyRename(df: DataFrame) extends Serializable {

    /** Rename columns of input dataFrame to name content in metadata  of structType pass by parameter
      *
      * @param structType used to get names to rename dataFrame columns
      */
    def renameColumns(structType: StructType): DataFrame = {
      renameColumnsDf(df, structType)
    }

  }

  private def renameColumnsDf(df: DataFrame, schema: Seq[StructField]): DataFrame = {
    schema
      .foldLeft(df) { (df, field) =>
        Try(field.metadata.getString("rename")) match {
          case Success(rename) =>
            df.withColumnRenamed(field.name, rename)
          case _ => df
        }
      }
  }

}
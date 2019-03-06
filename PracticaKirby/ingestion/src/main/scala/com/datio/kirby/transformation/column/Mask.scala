package com.datio.kirby.transformation.column

import java.sql.Date

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.config.{ConfigurationHandler, configurable}
import com.datio.kirby.errors.TRANSFORMATION_MASK_FIELD_NOT_EXISTS
import com.datio.kirby.util.ConversionTypeUtil
import com.typesafe.config.Config
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SparkSession}

import scala.util.Try

/**
  * Create masked column with default value.
  *
  * @param config config for mask masterization.
  */
@configurable("mask")
class Mask(val config: Config, spark: SparkSession) extends ColumnTransformation with ConversionTypeUtil {

  override def transform(col: Column): Column = {
    val (value, typeValue) = parametersForMaskColumn()
    logger.info("Masterization: Mask column : {} with value: {} of type: {}", columnName, value.toString, typeValue.toString)
    lit(value).cast(typeValue)
  }

  protected def outputSchema: StructType = ConfigurationHandler.outputSchema

  private def parametersForMaskColumn(): (Any, DataType) = {
    val typeColumn: DataType = Try(
      typeToCast(config.getString("dataType"))
    ).getOrElse(outputSchema.
      fields
      .find(_.name == columnName)
      .map(_.dataType)
      .getOrElse(throw new KirbyException(TRANSFORMATION_MASK_FIELD_NOT_EXISTS, columnName))
    )

    typeColumn match {
      case dataType: DataType if dataType.isInstanceOf[StringType] => ("X", StringType)
      case dataType: DataType if dataType.isInstanceOf[IntegerType] => (0, IntegerType)
      case dataType: DataType if dataType.isInstanceOf[DoubleType] => (0D, DoubleType)
      case dataType: DataType if dataType.isInstanceOf[LongType] => (0L, LongType)
      case dataType: DataType if dataType.isInstanceOf[FloatType] => (0F, FloatType)
      case dataType: DataType if dataType.isInstanceOf[DateType] => (new Date(0), DateType)
      case _ => ("XX", StringType)
    }
  }
}

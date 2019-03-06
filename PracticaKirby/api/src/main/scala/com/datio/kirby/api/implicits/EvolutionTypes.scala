package com.datio.kirby.api.implicits

import com.datio.kirby.api.exceptions.KirbyException
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Try

/** Resolve evolution from columns with differents types
  */
trait EvolutionTypes extends LazyLogging {

  /** implicit class used to evolve dataFrame column types
    *
    * @param df input dataFrame
    */
  implicit class EvolutionTypes(df: DataFrame) extends Serializable {

    /** Resolve evolution from input data frame. The columns that are struct will be transformed casting the
      * first value not null of each struct to the type of the first value of the struct. Example:
      * with the struct [String,Long,Int]
      * row ["1", null, null] => "1"
      * row [null, 12L, null] => "12"
      * row [null, null, 14] => "14"
      *
      * @return input data frame with columns evolved
      */
    def resolveEvolution: DataFrame = {
      resolveEvolutionDataTypes(df)
    }

  }

  private def resolveEvolutionDataTypes(df: DataFrame): DataFrame = {
    df.schema.foldLeft(df) { (df, field) =>
      field.dataType match {
        case StringType => df
        case StructType(subTypes) =>
          val structTypeDataTypes = subTypes.map(_.dataType).toList
          val finalDataType = subTypes.find(_.dataType != NullType).map(_.dataType).getOrElse(throw new RuntimeException(""))
          val castFunctions = createCastFunctions(structTypeDataTypes, finalDataType)
          val structTypeCastToString = udf((r: Row) => {
            Try {
              r.cast(castFunctions)
            }.getOrElse(None)
          }, finalDataType)

          df.withColumn(field.name, structTypeCastToString(df(field.name)).cast(finalDataType))
        case _ => df
      }
    }
  }

  /**
    * Given a list of dataTypes and a final type create a list of functions that cast from the elements of the list to
    * final type. Possible spark types are (only allowed types from avro schema):
    * boolean,int,long,float,double,number,string (bytes not included)
    *
    * @param structTypeDataTypes List of input types
    * @param finalDataType       result type
    * @return list of functions of casting
    */
  def createCastFunctions(structTypeDataTypes: List[DataType], finalDataType: DataType): List[(Any) => Any] = {

    def castToInt(fromDataType: DataType): (Any) => Any = fromDataType match {
      case (LongType) => (v: Any) => v.asInstanceOf[Long].toInt
      case (FloatType) => (v: Any) => v.asInstanceOf[Float].toInt
      case (DoubleType) => (v: Any) => v.asInstanceOf[Double].toInt
      case (StringType) => (v: Any) => v.asInstanceOf[String].toInt
    }

    def castToLong(fromDataType: DataType): (Any) => Any = fromDataType match {
      case (IntegerType) => (v: Any) => v.asInstanceOf[Int].toLong
      case (FloatType) => (v: Any) => v.asInstanceOf[Float].toLong
      case (DoubleType) => (v: Any) => v.asInstanceOf[Double].toLong
      case (StringType) => (v: Any) => v.asInstanceOf[String].toLong
    }

    def castToDouble(fromDataType: DataType): (Any) => Any = fromDataType match {
      case (IntegerType) => (v: Any) => v.asInstanceOf[Int].toDouble
      case (FloatType) => (v: Any) => v.asInstanceOf[Float].toString.toDouble
      case (LongType) => (v: Any) => v.asInstanceOf[Long].toDouble
      case (StringType) => (v: Any) => v.asInstanceOf[String].toDouble
    }

    def castToFloat(fromDataType: DataType): (Any) => Any = fromDataType match {
      case (IntegerType) => (v: Any) => v.asInstanceOf[Int].toFloat
      case (LongType) => (v: Any) => v.asInstanceOf[Long].toFloat
      case (DoubleType) => (v: Any) => v.asInstanceOf[Double].toString.toFloat
      case (StringType) => (v: Any) => v.asInstanceOf[String].toFloat
    }

    structTypeDataTypes.map(fromDataType => {
      (fromDataType, finalDataType) match {
        case (f, t) if f == t => (v: Any) => v
        case (f, IntegerType) => castToInt(f)
        case (f, LongType) => castToLong(f)
        case (f, FloatType) => castToFloat(f)
        case (f, DoubleType) => castToDouble(f)
        case (_, StringType) => (v: Any) => v.toString
        case (f, t) => throw new KirbyException(s"Unsupported evolution from $f to $t")
      }
    })
  }

}
package com.datio.kirby.config

import com.datio.kirby.api.Transformation
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.TRANSFORMATION_CLASS_NOT_IMPLEMENTED
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Create an appropriate Sequence of Transformation implementation after reading
  * the configuration.
  */
trait TransformationFactory extends LazyLogging with ConfigHelper {

  private lazy val transformations = getImplementations[Transformation]

  private val field = "field"

  /**
    * Read transformations from config.
    *
    * @param transformationConfig configuration to Transform
    * @return DataFrame after check.
    */
  def readTransformation(transformationConfig: Config)(implicit spark: SparkSession): Transformation = {
    getTransformation(transformationConfig)
  }

  /**
    * Expand a transformation with a regex in the config parameter 'field'
    *
    * @param columns        list of column names to search the pattern
    * @param transformation transformation with config to be expanded
    * @return sequence of cloned transformations with 'field' changed
    */
  def expandTransformation(columns: Array[String], transformation: Transformation)(implicit spark: SparkSession):
  Seq[Transformation] = {
    val regex = transformation.config.getString(field)
    for {
      columnName <- columns
      if columnName.matches(regex)
    } yield readTransformation(transformation.config.withValue(field, ConfigValueFactory.fromAnyRef(columnName)))(spark)
  }

  private def getTransformation(config: Config)(implicit spark: SparkSession): Transformation = {

    def simpleConstructor(clazz: Class[_]) =
      clazz.getConstructor(classOf[Config]).newInstance(config)

    def complexConstructor(clazz: Class[_]) =
      clazz.getConstructor(classOf[Config], classOf[SparkSession]).newInstance(config, spark)

    val transformationClazz = config.getString("type").toLowerCase() match {
      case "custom" => Class.forName(config.getString("class"))
      case other: String => transformations.get(other) match {
        case Some(clazz) => clazz
        case None => throw new KirbyException(TRANSFORMATION_CLASS_NOT_IMPLEMENTED, other)
      }
    }
    Try(simpleConstructor(transformationClazz))
      .getOrElse(complexConstructor(transformationClazz))
      .asInstanceOf[Transformation]
  }
}

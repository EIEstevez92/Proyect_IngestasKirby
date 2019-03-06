package com.datio.kirby.config

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.SCHEMA_PATH_MANDATORY
import com.typesafe.config.{Config, ConfigException}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.json4s.JValue

import scala.util.{Failure, Success, Try}

/**
  * Read schema from config.
  */
trait SchemaReader extends LazyLogging with FileReader {

  /** Read a avro schema from path param pass in config
    *
    * @param config    schema configuration
    * @param mandatory schema is mandatory or not (default true)
    * @return
    */
  def readSchema(config: Config, mandatory: Boolean = true, includeDeleted: Boolean = false, includeMetadata: Boolean = false): StructType = {
    lazy val validation = Try(config.getBoolean("validation")).getOrElse(true) || sys.env.getOrElse("kirby_validation_enabled", "true").toBoolean
    Try(config.getString("path")) match {
      case _ if !validation =>
        logger.warn("SchemaReader: Validation disabled")
        StructType(Seq())
      case Failure(_: ConfigException.Missing) if !mandatory => StructType(Seq())
      case Failure(_) => throw new KirbyException(SCHEMA_PATH_MANDATORY, config.toString)
      case Success(path) => readStructType(path, includeDeleted, includeMetadata)
    }
  }

  /** Read a json schema from path and transform it to StructType
    *
    * @param path            path of schema
    * @param includeDeleted  flag to include or not deleted columns
    * @param includeMetadata flag to include or not metadata columns
    * @return
    */
  protected def readStructType(path: String, includeDeleted: Boolean, includeMetadata: Boolean): StructType = {
    import org.json4s._
    val jsonContent: JValue = readJson(path)
    implicit val formats: Formats = DefaultFormats + FieldSerializer[StructSchema]() + new JsonSchemaFieldSerializer()
    jsonContent.extract[StructSchema].toStructType(includeDeleted, includeMetadata)
  }

  /** Read json content from path
    *
    * @param path path of json
    * @return the JValue of the content path
    */
  protected def readJson(path: String): JValue = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    val content = getContentFromPath(path)
    parse(removeBomCharacter(content))
  }

  /**
    * Schemas could contain a Byte Order Mark (BOM) that is the Unicode char '\uFEFF'
    * This character breaks the json conversion and we need to remove it.
    *
    * @param content schema content in string
    * @return schema content without Byte Order Mark
    */
  protected def removeBomCharacter(content: String): String = {
    if (content.startsWith('\uFEFF'.toString)) {
      content.substring(1)
    } else {
      content
    }
  }
}
package com.datio.kirby

import com.datio.kirby.api.{Input, Output, Transformation}
import com.datio.kirby.util.ConversionTypeUtil
import com.typesafe.config.Config
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.{CustomSerializer, DefaultFormats, Formats}

import scala.reflect.runtime.universe._

package object config {

  /**
    * Case class to store kirby configuration
    *
    * @param input           input dataFrame
    * @param output          output dataFrame
    * @param outputSchema    use to validate output
    * @param transformations list of transformation to apply
    */
  case class KirbyConfig(input: Input,
                         output: Output,
                         outputSchema: StructType,
                         transformations: Seq[Transformation],
                         sparkOptions: Map[String, Any])

  /**
    * Trait to define a spark option obtained from kirby configuration
    */
  trait SparkOption[T] {
    val sparkKey: String
    val confKey: String

    def getValue(config: Config)(implicit tag: TypeTag[T]): Option[T]
  }

  /**
    * Case class to define a boolean spark option obtained from kirby configuration
    *
    * @param sparkKey concrete spark option
    * @param confKey  concrete kirby option
    */
  case class BooleanSparkOption(sparkKey: String, confKey: String) extends SparkOption[Boolean] with SparkOptionReader {
    override def getValue(config: Config)(implicit tag: TypeTag[Boolean]): Option[Boolean] =
      readOption[Boolean](confKey, config, config.getBoolean)
  }

  /**
    * Case class to define a string spark option obtained from kirby configuration
    *
    * @param sparkKey concrete spark option
    * @param confKey  concrete kirby option
    */
  case class StringSparkOption(sparkKey: String, confKey: String) extends SparkOption[String] with SparkOptionReader {
    override def getValue(config: Config)(implicit tag: TypeTag[String]): Option[String] =
      readOption[String](confKey, config, config.getString)
  }

  /**
    * Case class to define a long spark option obtained from kirby configuration
    *
    * @param sparkKey concrete spark option
    * @param confKey  concrete kirby option
    */
  case class LongSparkOption(sparkKey: String, confKey: String) extends SparkOption[Long] with SparkOptionReader {
    override def getValue(config: Config)(implicit tag: TypeTag[Long]): Option[Long] =
      readOption[Long](confKey, config, config.getLong)
  }

  /** json schema to parse or validate DataFrames
    *
    * @param namespace string that qualifies the name
    * @param name      name of the table
    * @param doc       string providing documentation to the user of this schema
    * @param database  name of the database
    * @param `type`    defining a schema, or a JSON string naming a record definition.
    * @param fields    listing fields (required)
    */
  case class StructSchema(namespace: Option[String],
                          name: Option[String],
                          doc: Option[String],
                          database: Option[String],
                          `type`: Option[String],
                          fields: Seq[StructSchemaField]) extends ConversionTypeUtil {


    /** Converter: from JsonSchema to StructType
      *
      * @param includeDeleted  include fields with deleted flag = true (default false)
      * @param includeMetadata include fields with metadata flag = true (default false)
      * @return StructType
      */
    def toStructType(includeDeleted: Boolean = false, includeMetadata: Boolean = false): StructType = {
      def getFirstType(field: StructSchemaField) = field.`type`.filter(_ != "null").head

      StructType(fields
        .filter(includeDeleted || !_.deleted.getOrElse(false))
        .filter(includeMetadata || !_.metadata.getOrElse(false))
        .map(field => {
          StructField(
            field.name,
            typeToCast(getFirstType(field)),
            field.`type`.contains("null"), {
              val metadata = new MetadataBuilder()
              field.logicalFormat.map(logicalFormat => metadata.putString("logicalFormat", logicalFormat))
              field.format.map(format => metadata.putString("format", format))
              field.locale.map(locale => metadata.putString("locale", locale))
              field.typeFormat.map(typeFormat => metadata.putString("typeFormat", typeFormat))
              field.rename.map(typeFormat => metadata.putString("rename", typeFormat))
              metadata.build
            }
          )
        }))
    }

  }

  /** json object to define a schema field
    *
    * @param name       name of the field
    * @param `type`     string naming a record definition
    * @param default    default value for this field
    * @param legacyName default value for this field
    * @param logicalFormat origin type to this field
    * @param format     pattern to read input timestamps
    * @param locale     locale to read timestamps or numbers
    * @param typeFormat key to specify format type (available 'iso8601'(default), 'java')
    * @param rename     name to use with this field
    * @param deleted    flag to know if a column have been deleted of the primitive input formats
    * @param metadata   flag to know if a column is metadata (is introduced in the avro format (it does not come in the primitive format)
    */
  case class StructSchemaField(name: String,
                               `type`: Seq[String],
                               default: Any,
                               legacyName: Option[String],
                               logicalFormat: Option[String],
                               format: Option[String],
                               locale: Option[String],
                               typeFormat: Option[String],
                               rename: Option[String],
                               deleted: Option[Boolean],
                               metadata: Option[Boolean])

  implicit val formats: Formats = DefaultFormats

  /** json4s serializer to read the schema json fields and return instances of StructSchemaField
    *
    */
  class JsonSchemaFieldSerializer extends CustomSerializer[StructSchemaField](
    _ => ( {
      case jsonObj: JObject =>
        val name = (jsonObj \ "name").extract[String]
        val legacyName = (jsonObj \ "legacyName").extractOpt[String]
        val `type` = (jsonObj \ "type").extract[Any] match {
          case l: Seq[String] => l
          case s: String => Seq(s)
          case _ => throw new RuntimeException("")
        }
        val default = (jsonObj \ "default").extract[Any]
        val logicalFormat = (jsonObj \ "logicalFormat").extractOpt[String]
        val format = (jsonObj \ "format").extractOpt[String]
        val locale = (jsonObj \ "locale").extractOpt[String]
        val typeFormat = (jsonObj \ "typeFormat").extractOpt[String]
        val rename = (jsonObj \ "rename").extractOpt[String]
        val deleted = (jsonObj \ "deleted").extractOpt[Boolean]
        val metadata = (jsonObj \ "metadata").extractOpt[Boolean]
        StructSchemaField(name, `type`, default, legacyName, logicalFormat, format, locale, typeFormat, rename, deleted, metadata)
    }, {
      case jsonSchemaField: StructSchemaField =>
        ("name" -> jsonSchemaField.name) ~
          ("type" -> jsonSchemaField.`type`) ~
          ("default" -> jsonSchemaField.default.toString) ~
          ("legacyName" -> jsonSchemaField.legacyName) ~
          ("logicalFormat" -> jsonSchemaField.logicalFormat) ~
          ("format" -> jsonSchemaField.format) ~
          ("locale" -> jsonSchemaField.locale) ~
          ("typeFormat" -> jsonSchemaField.typeFormat) ~
          ("rename" -> jsonSchemaField.rename) ~
          ("deleted" -> jsonSchemaField.deleted) ~
          ("metadata" -> jsonSchemaField.metadata)
    }
    ))

}
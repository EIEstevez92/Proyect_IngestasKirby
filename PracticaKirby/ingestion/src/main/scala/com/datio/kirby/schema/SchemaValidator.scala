package com.datio.kirby.schema

import com.datio.kirby.config.validator.InvalidSchemaException
import com.datio.kirby.errors.SCHEMA_DIFF_ERROR
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}


/**
  * Apply validate to dataFrame finish.
  */
trait SchemaValidator extends LazyLogging {

  val DELIMITER = ", "

  /**
    * Validate DF.
    *
    * @param df              DataFrame to be validated.
    * @param schemaValidator StructType to perform the validation.
    */
  def validateDF(df: DataFrame, schemaValidator: StructType): Unit = {
    def mapTypes(schema: StructType) = schema.map(v => v.name -> v.dataType).toSet

    if (shouldValidateDF(schemaValidator)) {
      val mapToValidate = mapTypes(df.schema)
      val mapValidator = mapTypes(schemaValidator)

      logger.info(s"Schema to be validated: ${mapToValidate.mkString(DELIMITER)} must be equal than reference schema: ${mapValidator.mkString(DELIMITER)}")

      val leftOverColumns = df.schema.map(_.name).toSet -- schemaValidator.map(_.name).toSet
      val leftOverColumnsError = Option(leftOverColumns).filter(_.nonEmpty).map(v => s"DataFrame schema left over columns: ${v.mkString(DELIMITER)}")

      val lackColumns = schemaValidator.map(_.name).toSet -- df.schema.map(_.name).toSet
      val lackColumnsError = Option(lackColumns).filter(_.nonEmpty).map(v => s"DataFrame schema lack columns: ${v.mkString(DELIMITER)}")

      val columnsDiffType = for {
        columnToValidate <- mapToValidate
        columnValidator <- mapValidator
        if columnToValidate._1 == columnValidator._1
        if columnToValidate._2 != columnValidator._2
      } yield (columnToValidate._1, columnToValidate._2, columnValidator._2)
      val columnsDiffTypeError = Option(columnsDiffType).filter(_.nonEmpty).map(v => s"DataFrame schema have columns with incorrect dataType: " +
        s"${v.map({ case (name, typeDF, typeVal) => s"$name($typeDF) should be $typeVal" }).mkString(DELIMITER)}")

      val errors = (leftOverColumnsError ++ lackColumnsError ++ columnsDiffTypeError).mkString(".\n")
      if (errors.nonEmpty) {
        throw new InvalidSchemaException(SCHEMA_DIFF_ERROR, errors)
      }
    }
  }

  /**
    * Apply not nullable to mandatory columns in DF.
    *
    * @param df              DataFrame to be validated.
    * @param schemaValidator StructType to perform the validation.
    */
  def applyMandatoryColumns(df: DataFrame, schemaValidator: StructType): DataFrame = {
    val schema = df.schema
    val newMandatoryColumns = for {
      mandatoryColumn <- schemaValidator.filter(!_.nullable)
      newMandatoryColumn <- schema.filter(_.name == mandatoryColumn.name).filter(_.nullable)
    } yield newMandatoryColumn.name

    if (newMandatoryColumns.isEmpty) {
      df
    } else {
      val newSchema = StructType(schema.map {
        case StructField(c, t, _, m) if newMandatoryColumns.contains(c) => StructField(c, t, nullable = false, m)
        case y: StructField => y
      })
      df.sqlContext.createDataFrame(df.rdd, newSchema)
    }
  }

  private def shouldValidateDF(dfSchema: StructType) = dfSchema.fields.nonEmpty

}

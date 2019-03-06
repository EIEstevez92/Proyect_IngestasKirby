package com.datio.kirby.schema

import com.datio.kirby.config.validator.InvalidSchemaException
import com.datio.kirby.testUtils.InitSparkSessionFeature
import org.apache.spark.SparkException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class SchemaValidatorTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with SchemaValidator {

  feature("schema validation in output") {
    import spark.implicits._

    scenario("schemas are identical and match") {
      Given("a struct field with two columns")
      val reference = StructType(Seq(StructField("A", StringType), StructField("B", IntegerType)))

      When("validate against a correct DataFrame")
      val df = Seq(("ana", 16)).toDF("A", "B")

      Then("should not return any error")
      validateDF(df, reference)
    }

    scenario("schemas are different in length") {
      Given("a struct field with two columns")
      val reference = StructType(Seq(StructField("A", StringType), StructField("B", IntegerType), StructField("D", IntegerType)))

      When("validate against a DataFrame with three columns")
      val df = Seq(("ana", 16, "Madrid")).toDF("A", "B", "C")

      Then("should return an error")
      val ex = intercept[InvalidSchemaException] {
        validateDF(df, reference)
      }
      ex.getMessage shouldBe "125 - Schema Validation Error: DataFrame schema doesn't match with validation schema." +
        "\nDataFrame schema left over columns: C." +
        "\nDataFrame schema lack columns: D"
    }

    scenario("schemas are different because types not match") {
      Given("a struct field with two columns")
      val reference = StructType(Seq(StructField("A", StringType), StructField("B", IntegerType)))

      When("validate against a DataFrame with other type")
      val df = Seq(("ana", "Madrid")).toDF("A", "B")

      Then("should return an error")
      val ex = intercept[InvalidSchemaException] {
        validateDF(df, reference)
      }
      ex.getMessage shouldBe "125 - Schema Validation Error: DataFrame schema doesn't match with validation schema." +
        "\nDataFrame schema have columns with incorrect dataType: B(StringType) should be IntegerType"
    }

  }
  feature("Apply mandatory columns") {
    import spark.implicits._

    scenario("DataFrame not contain null on mandatory columns") {
      Given("a struct field with two columns")
      val reference = StructType(Seq(StructField("A", StringType, nullable = false), StructField("B", StringType, nullable = true)))

      Given("dataFrame with correct data and schema")
      val df = Seq(("ana", "ana"), ("ana", null)).toDF("A", "B")

      When("apply Mandatory to dataFrame")
      val dfResult = applyMandatoryColumns(df, reference)

      Then("should not return any error")
      dfResult.count
    }

    scenario("schemas are different and DataFrame is incorrect") {
      Given("a struct field with two columns")
      val reference = StructType(Seq(StructField("A", StringType, nullable = false), StructField("B", StringType, nullable = false)))

      Given("dataFrame with correct data and schema")
      val df = Seq(("ana", "ana"), ("ana", null)).toDF("A", "B")

      When("apply Mandatory to dataFrame")
      val dfResult = applyMandatoryColumns(df, reference)

      Then("should return error")
      val ex = intercept[SparkException] {
        dfResult.count
      }
      ex.getCause.getCause.getMessage shouldBe "The 1th field 'B' of input row cannot be null."
    }
  }
}

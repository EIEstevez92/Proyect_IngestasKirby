package com.datio.kirby.transformation.column

import java.sql.Date

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class MaskTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  private val baseTransformationConfig = ConfigFactory.parseString(
    """
      |{
      |  type = "mask"
      |  field = "fieldToMask"
      |}
    """.stripMargin)

  class MaskMocked(override val config: Config, spark: SparkSession)(override val outputSchema: StructType)
    extends Mask(config, spark)

  feature("masked column") {

    scenario("mask integer field") {
      Given("configuration")

      val maskConfig = baseTransformationConfig
      val outputSchemaMock = StructType(Seq(StructField("fieldToMask", IntegerType)))

      Given("column to parse")

      import spark.implicits._
      val df = List(MaskIntegerEntity(1), MaskIntegerEntity(2), MaskIntegerEntity(0)).toDF("fieldToMask")

      When("apply transformations")

      val dfResult = new MaskMocked(maskConfig, spark)(outputSchemaMock).transform(df)

      Then("add literalfield")
      assert(dfResult
        .select("fieldToMask")
        .collect()
        .forall(
          _.getAs[Int]("fieldToMask") === 0
        )
      )
    }

    scenario("mask double field") {
      Given("configuration")

      val maskConfig = baseTransformationConfig
      val outputSchemaMock = StructType(Seq(StructField("fieldToMask", DoubleType)))

      Given("column to parse")

      import spark.implicits._
      val df = List(MaskDoubleEntity(1D), MaskDoubleEntity(2D), MaskDoubleEntity(0D)).toDF("fieldToMask")

      When("apply transformations")

      val dfResult = new MaskMocked(maskConfig, spark)(outputSchemaMock).transform(df)

      Then("add literalfield")
      assert(dfResult
        .select("fieldToMask")
        .collect()
        .forall(
          _.getAs[Double]("fieldToMask") === 0D
        )
      )
    }

    scenario("mask long field") {
      Given("configuration")

      val maskConfig = baseTransformationConfig
      val outputSchemaMock = StructType(Seq(StructField("fieldToMask", LongType)))

      Given("column to parse")

      import spark.implicits._
      val df = List(MaskLongEntity(1L), MaskLongEntity(2L), MaskLongEntity(0L)).toDF("fieldToMask")

      When("apply transformations")

      val dfResult = new MaskMocked(maskConfig, spark)(outputSchemaMock).transform(df)

      Then("add literalfield")
      assert(dfResult
        .select("fieldToMask")
        .collect()
        .forall(
          _.getAs[Long]("fieldToMask") === 0
        )
      )
    }

    scenario("mask float field") {
      Given("configuration")

      val maskConfig = baseTransformationConfig
      val outputSchemaMock = StructType(Seq(StructField("fieldToMask", FloatType)))

      Given("column to parse")

      import spark.implicits._
      val df = List(MaskFloatEntity(1F), MaskFloatEntity(2F), MaskFloatEntity(0F)).toDF("fieldToMask")

      When("apply transformations")

      val dfResult = new MaskMocked(maskConfig, spark)(outputSchemaMock).transform(df)

      Then("add literalfield")
      assert(dfResult
        .select("fieldToMask")
        .collect()
        .forall(
          _.getAs[Float]("fieldToMask") === 0
        )
      )
    }

    scenario("mask date field") {
      Given("configuration")

      val maskConfig = baseTransformationConfig
      val outputSchemaMock = StructType(Seq(StructField("fieldToMask", DateType)))

      Given("column to parse")

      import spark.implicits._
      val df = List(MaskDateEntity(new Date(0)), MaskDateEntity(new Date(1)), MaskDateEntity(new Date(2))).toDF("fieldToMask")

      When("apply transformations")

      val dfResult = new MaskMocked(maskConfig, spark)(outputSchemaMock).transform(df)

      Then("add literalfield")
      assert(dfResult
        .select("fieldToMask")
        .collect()
        .forall(
          _.getAs[Date]("fieldToMask").isInstanceOf[Date]
        )
      )
    }

    scenario("mask other field") {
      Given("configuration")

      val maskConfig = baseTransformationConfig
      val outputSchemaMock = StructType(Seq(StructField("fieldToMask", BooleanType)))

      Given("column to parse")

      import spark.implicits._
      val df = List(MaskBooleanEntity(true), MaskBooleanEntity(false), MaskBooleanEntity(false)).toDF("fieldToMask")

      When("apply transformations")

      val dfResult = new MaskMocked(maskConfig, spark)(outputSchemaMock).transform(df)

      Then("add literalfield")
      assert(dfResult
        .select("fieldToMask")
        .collect()
        .forall(
          _.getAs[String]("fieldToMask") === "XX"
        )
      )
    }

  }
}

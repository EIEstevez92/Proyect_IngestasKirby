package com.datio.kirby.api.implicits

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class EvolutionTypesTest extends InitSparkSessionFeature with EvolutionTypes with GivenWhenThen with Matchers
  with FileTestUtil {

  feature("Apply type Date to Data Frames follow the schema") {

    scenario("Evolution types to Int") {
      Given("A schema with structType with int type as first type of the structType")
      val schema = StructType(List(
        StructField("toEvolve", StructType(Seq(
          StructField("member0", IntegerType, nullable = true),
          StructField("member1", StringType, nullable = true),
          StructField("member2", LongType, nullable = true),
          StructField("member3", FloatType, nullable = true),
          StructField("member4", DoubleType, nullable = true))
        ), nullable = true)))

      Given("A dataFrame according the schema")
      val data = List(
        Row(Row(1, null, null, null, null)),
        Row(Row(null, "2", null, null, null)),
        Row(Row(null, null, 3L, null, null)),
        Row(Row(null, null, null, 4.1F, null)),
        Row(Row(null, null, null, null, 5.9D))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      When("Apply evolution")
      val dfEvolved = df.resolveEvolution

      Then("StructType are transformed to IntType")
      dfEvolved.collect().map(_.getAs[Integer]("toEvolve")).toSet shouldBe Set(1, 2, 3, 4, 5)
    }

    scenario("Evolution types to Long") {
      Given("A schema with structType with long type as first type of the structType")
      val schema = StructType(List(
        StructField("toEvolve", StructType(Seq(
          StructField("member0", LongType, nullable = true),
          StructField("member1", StringType, nullable = true),
          StructField("member2", IntegerType, nullable = true),
          StructField("member3", FloatType, nullable = true),
          StructField("member4", DoubleType, nullable = true))
        ), nullable = true)))

      Given("A dataFrame according the schema")
      val data = List(
        Row(Row(1L, null, null, null, null)),
        Row(Row(null, "2", null, null, null)),
        Row(Row(null, null, 3, null, null)),
        Row(Row(null, null, null, 4.1F, null)),
        Row(Row(null, null, null, null, 5.9D))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      When("Apply evolution")
      val dfEvolved = df.resolveEvolution

      Then("StructType are transformed to LongType")
      dfEvolved.collect().map(_.getAs[Long]("toEvolve")).toSet shouldBe Set(1L, 2L, 3L, 4L, 5L)
    }

    scenario("Evolution types to Double") {
      Given("A schema with structType with double type as first type of the structType")
      val schema = StructType(List(
        StructField("toEvolve", StructType(Seq(
          StructField("member0", DoubleType, nullable = true),
          StructField("member1", StringType, nullable = true),
          StructField("member2", IntegerType, nullable = true),
          StructField("member3", FloatType, nullable = true),
          StructField("member4", LongType, nullable = true))
        ), nullable = true)))

      Given("A dataFrame according the schema")
      val data = List(
        Row(Row(5.9D, null, null, null, null)),
        Row(Row(null, "2.23", null, null, null)),
        Row(Row(null, null, 3, null, null)),
        Row(Row(null, null, null, 4.1F, null)),
        Row(Row(null, null, null, null, 1L))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      When("Apply evolution")
      val dfEvolved = df.resolveEvolution

      Then("StructType are transformed to LongType")
      dfEvolved.collect().map(_.getAs[Double]("toEvolve")).toSet shouldBe Set(1.0D, 2.23D, 3D, 4.1D, 5.9D)
    }

    scenario("Evolution types to Float") {
      Given("A schema with structType with float type as first type of the structType")
      val schema = StructType(List(
        StructField("toEvolve", StructType(Seq(
          StructField("member0", FloatType, nullable = true),
          StructField("member1", StringType, nullable = true),
          StructField("member2", IntegerType, nullable = true),
          StructField("member3", DoubleType, nullable = true),
          StructField("member4", LongType, nullable = true))
        ), nullable = true)))

      Given("A dataFrame according the schema")
      val data = List(
        Row(Row(4.1F, null, null, null, null)),
        Row(Row(null, "2.23", null, null, null)),
        Row(Row(null, null, 3, null, null)),
        Row(Row(null, null, null, 5.9D, null)),
        Row(Row(null, null, null, null, 1L))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      When("Apply evolution")
      val dfEvolved = df.resolveEvolution

      Then("StructType are transformed to LongType")
      dfEvolved.collect().map(_.getAs[Float]("toEvolve")).toSet shouldBe Set(1.0F, 2.23F, 3F, 4.1F, 5.9F)
    }

    scenario("Evolution types to String") {
      Given("A schema with structType with float type as first type of the structType")
      val schema = StructType(List(
        StructField("toEvolve", StructType(Seq(
          StructField("member0", StringType, nullable = true),
          StructField("member1", FloatType, nullable = true),
          StructField("member2", IntegerType, nullable = true),
          StructField("member3", DoubleType, nullable = true),
          StructField("member4", LongType, nullable = true))
        ), nullable = true)))

      Given("A dataFrame according the schema")
      val data = List(
        Row(Row("2.23", null, null, null, null)),
        Row(Row(null, 4.1F, null, null, null)),
        Row(Row(null, null, 3, null, null)),
        Row(Row(null, null, null, 5.9D, null)),
        Row(Row(null, null, null, null, 1L))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      When("Apply evolution")
      val dfEvolved = df.resolveEvolution

      Then("StructType are transformed to LongType")
      dfEvolved.collect().map(_.getAs[String]("toEvolve")).toSet shouldBe Set("1", "2.23", "3", "4.1", "5.9")
    }

    scenario("Evolution unsupported types") {
      Given("A schema with structType with float type as first type of the structType")
      val schema = StructType(List(
        StructField("toEvolve", StructType(Seq(
          StructField("member0", BooleanType, nullable = true),
          StructField("member1", FloatType, nullable = true),
          StructField("member2", IntegerType, nullable = true),
          StructField("member3", DoubleType, nullable = true),
          StructField("member4", LongType, nullable = true))
        ), nullable = true)))

      Given("A dataFrame according the schema")
      val data = List(
        Row(Row(true, null, null, null, null)),
        Row(Row(null, 4.1F, null, null, null)),
        Row(Row(null, null, 3, null, null)),
        Row(Row(null, null, null, 5.9D, null)),
        Row(Row(null, null, null, null, 1L))
      )
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      When("Apply evolution")
      val e = intercept[KirbyException] {
        df.resolveEvolution
      }
      Then("StructType are transformed to LongType")
      e.getMessage shouldBe "Unsupported evolution from FloatType to BooleanType"
    }
  }
}


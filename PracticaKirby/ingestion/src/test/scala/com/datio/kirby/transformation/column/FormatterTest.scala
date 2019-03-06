package com.datio.kirby.transformation.column

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class FormatterTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Format column") {

    scenario("cast to String") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "int"
          |        type = "formatter"
          |        typeToCast = "string"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row(1) :: Row(1) :: Row(1) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("int", IntegerType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "int").get.dataType === StringType)

      Then("should cast to String")
      assert(dfResult
        .select("int")
        .collect()
        .forall(
          _.getAs[String]("int") === "1"
        )
      )
    }

    scenario("cast to Integer") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "string"
          |        type = "formatter"
          |        typeToCast = "int"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("1") :: Row("1") :: Row("1") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("string", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "string").get.dataType === IntegerType)

      Then("should cast to Integer")
      assert(dfResult
        .select("string")
        .collect()
        .forall(
          _.getAs[Int]("string") === 1
        )
      )
    }

    scenario("cast to Date") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "string"
          |        type = "formatter"
          |        typeToCast = "date"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("2017-07-16") :: Row("2017-07-16") :: Row("2017-07-16") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("string", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "string").get.dataType === DateType)

      Then("should cast to Date")
      assert(dfResult
        .select("string")
        .collect()
        .forall(
          _.getAs[java.util.Date]("string") != None.orNull
        )
      )
    }

    scenario("cast to Date malformed") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "string"
          |        type = "formatter"
          |        typeToCast = "date"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("malformed") :: Row("malformed") :: Row("malformed") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("string", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "string").get.dataType === DateType)

      Then("should cast to Date malformed")
      assert(dfResult
        .select("string")
        .collect()
        .forall(
          _.getAs[java.util.Date]("string") == None.orNull
        )
      )
    }

    scenario("cast to same type") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "long"
          |        type = "formatter"
          |        typeToCast = "long"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row(10L) :: Row(10L) :: Row(10L) :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("long", LongType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "long").get.dataType === LongType)

      Then("should cast to Date malformed")
      assert(dfResult
        .select("long")
        .collect()
        .forall(
          _.getAs[Long]("long") == 10L
        )
      )
    }

    scenario("cast to Float with replacements") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "string"
          |        type = "formatter"
          |        typeToCast = "float"
          |        replacements = [
          |          {
          |            pattern = "[a-zA-Z]"
          |            replacement = ""
          |          }
          |        ]
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("A1.b") :: Row("1.fe0") :: Row("QQ1") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("string", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "string").get.dataType === FloatType)

      Then("should cast to Float")
      assert(dfResult
        .select("string")
        .collect()
        .forall(
          _.getAs[Float]("string") === 1.0f
        )
      )
    }

    scenario("cast to Date with replacements") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "string"
          |        type = "formatter"
          |        typeToCast = "date"
          |        replacements = [
          |          {
          |            pattern = "(\\d{4})(\\d{2})(\\d{2})(.*)"
          |            replacement = "$1-$2-$3"
          |          }
          |        ]
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("2017061221.36.31.172496") :: Row("2017061221.36.31.172496") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("string", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "string").get.dataType === DateType)

      Then("should cast to Date")
      assert(dfResult
        .select("string")
        .collect()
        .forall(
          _.getAs[java.util.Date]("string") != None.orNull
        )
      )
    }

    scenario("cast to None.orNullable Double with replacements") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "string"
          |        type = "formatter"
          |        typeToCast = "double"
          |        replacements = [
          |          {
          |            pattern = ".*"
          |            replacement = ""
          |          }
          |        ]
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("1.2") :: Row("1.2") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("string", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "string").get.dataType === DoubleType)

      Then("should cast to None.orNullable Double")
      assert(dfResult
        .select("string")
        .collect()
        .forall(
          _.getAs[Double]("string") == None.orNull
        )
      )
    }

    scenario("cast to Boolean with replacements") {
      Given("config")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "string"
          |        type = "formatter"
          |        typeToCast = "boolean"
          |        replacements = [
          |          {
          |            pattern = "[0-9]"
          |            replacement = ""
          |          },
          |          {
          |            pattern = "[a-z]"
          |            replacement = ""
          |          },
          |          {
          |            pattern = "V"
          |            replacement = "True"
          |          },
          |          {
          |            pattern = "N"
          |            replacement = "True"
          |          }
          |        ]
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("11234Vadybn") :: Row("1Nasd2") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("string", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)
      assert(dfResult.schema.find(_.name == "string").get.dataType === BooleanType)

      Then("should cast to Boolean")
      assert(dfResult
        .select("string")
        .collect()
        .forall(
          _.getAs[Boolean]("string")
        )
      )
    }
  }
}

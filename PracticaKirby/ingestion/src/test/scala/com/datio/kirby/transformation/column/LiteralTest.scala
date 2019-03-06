package com.datio.kirby.transformation.column

import java.util.Date

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LiteralTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("literal column") {

    scenario("literal column with cast to String") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "X"
          |        defaultType = "string"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[String]("text").toString === "X"
        )
      )
    }

    scenario("literal column with cast to int") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "0"
          |        defaultType = "int"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[Int]("text") === 0
        )
      )
    }

    scenario("literal column with cast to Long") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "0"
          |        defaultType = "long"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[Long]("text") === 0L
        )
      )
    }

    scenario("literal column with cast to Double") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "0"
          |        defaultType = "double"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[Double]("text") === 0D
        )
      )
    }

    scenario("literal column with cast to Float") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "0"
          |        defaultType = "float"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[Float]("text") === 0F
        )
      )
    }

    scenario("literal column with cast to Boolean") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "true"
          |        defaultType = "boolean"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[Boolean]("text") === true
        )
      )
    }

    scenario("literal column with cast to Date") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "2017-01-01"
          |        defaultType = "date"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[Date]("text").toString === "2017-01-01"
        )
      )
    }

    scenario("literal column with cast to Decimal with scale") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "200.22"
          |        defaultType = "decimal(10,2)"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[java.math.BigDecimal]("text") === new java.math.BigDecimal("200.22")
        )
      )
    }

    scenario("literal column with cast to Decimal without scale") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "200.22"
          |        defaultType = "decimal(10)"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text field is integer with default value")
      assert(dfResult
        .select("text")
        .collect()
        .forall(
          _.getAs[java.math.BigDecimal]("text") === new java.math.BigDecimal("200")
        )
      )
    }

    scenario("literal column with cast to unknown type") {
      Given("the following configuration")

      val config = ConfigFactory.parseString(
        """
          |      {
          |        field = "text"
          |        type = "literal"
          |        default = "unknown"
          |        defaultType = "unknown"
          |      }
        """.stripMargin
      )

      Given("column to parse")

      val columnToParse = Row("aa") :: Row("bb") :: Row("") :: Nil

      val df = spark.createDataFrame(columnToParse.asJava,
        StructType(
          StructField("text", StringType) :: Nil
        )
      )

      When("apply transformations")

      intercept[KirbyException](
        readTransformation(config)(spark).transform(df)
      )
    }

  }
}

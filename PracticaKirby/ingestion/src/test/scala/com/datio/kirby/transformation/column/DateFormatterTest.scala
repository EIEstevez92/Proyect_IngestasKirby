package com.datio.kirby.transformation.column

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Locale

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.TimestampType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class DateFormatterTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  feature("Format date") {
    scenario("check parse date correctly") {
      Given("config")

      val format = "dd/MMM/yy"
      val config = ConfigFactory.parseString(
        s"""      {
           |        field = "date"
           |        type = "dateformatter"
           |        format = $format
           |        operation = parse
           |        locale = es
           |      }
        """.stripMargin
      )

      Given("column to parse")
      import spark.implicits._
      val df = List("31/Dic/16", "01/Dic/16", "01/Ene/17").toDF("date")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be change to date when correct")
      val sdf = new SimpleDateFormat(format)
      dfResult.select("date").collect.map(_.getAs[Date]("date").toString).toSet shouldBe Set("2016-12-31","2016-12-01", "2017-01-01")

    }

    scenario("check parse timestamp correctly") {
      Given("config")

      val format = "dd/MMM/yy HH:mm:ss"
      val config = ConfigFactory.parseString(
        s"""      {
           |        field = "date"
           |        type = "dateformatter"
           |        format = "$format"
           |        operation = parseTimestamp
           |        locale = es
           |      }
        """.stripMargin
      )

      Given("column to parse")
      import spark.implicits._
      val df = List("31/Dic/16 12:11:59", "01/Dic/16 12:11:59", "01/Ene/17 12:11:59").toDF("date")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be change to date when correct")
      val sdf = new SimpleDateFormat(format)
      dfResult.select("date").collect.map(_.getAs[java.sql.Timestamp]("date").toString).toSet shouldBe Set("2016-12-31 12:11:59.0","2016-12-01 12:11:59.0", "2017-01-01 12:11:59.0")

    }

    scenario("check format date correctly") {
      Given("config")

      val format = "dd/MMM/yy"

      val config = ConfigFactory.parseString(
        s"""      {
           |        field = "date"
           |        type = "dateformatter"
           |        format = $format
           |        operation = format
           |        locale = es
           |      }
        """.stripMargin
      )

      Given("column to parse")
      import spark.implicits._
      val df = List("2016-12-31", "2017-01-21").toDF("date")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be change to date when correct")
      dfResult.select("date").collect.map(_.getAs[String]("date")).toSet shouldBe Set("31/dic/16", "21/ene/17")

    }

    scenario("check format timestamp correctly") {
      Given("config")

      val format = "dd/MMM/yy HH:mm:ss"

      val config = ConfigFactory.parseString(
        s"""      {
           |        field = "timestamp"
           |        type = "dateformatter"
           |        format = "$format"
           |        operation = format
           |        locale = es
           |      }
        """.stripMargin
      )

      Given("column to parse")
      import spark.implicits._
      val df = List("2016-12-31 12:11:59.123", "2017-01-21 12:11:59.123").toDF("str").withColumn("timestamp", 'str.cast(TimestampType))

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be change to date when correct")
      dfResult.select("timestamp").collect.map(_.getAs[String]("timestamp")).toSet shouldBe Set("31/dic/16 12:11:59", "21/ene/17 12:11:59")

    }

    scenario("check reformat date correctly") {
      Given("config")

      val format = "dd/MMM/yy hh:mm:ss"
      val reformat = "yyyy-MMM-dd HH:mm:ss"

      val config = ConfigFactory.parseString(
        s"""      {
           |        field = "date"
           |        type = "dateformatter"
           |        format = "$format"
           |        reformat = "$reformat"
           |        operation = reformat
           |        locale = "es_ES"
           |        relocale = "de_DE"
           |      }
        """.stripMargin
      )

      Given("column to parse")
      import spark.implicits._
      val df = List("31/Dic/16 21:07:11", "01/Dic/16 17:07:11").toDF("date")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be change to date when correct")
      dfResult.select("date").collect.map(_.getAs[String]("date")).toSet shouldBe Set("2016-Dez-31 21:07:11", "2016-Dez-01 17:07:11")

    }

    scenario("Reformat to extract hour") {
      Given("config")

      val format = "dd/MM/yy hh:mm:ss"
      val reformat = "HH"

      val config = ConfigFactory.parseString(
        s"""      {
           |        field = "date"
           |        type = "dateformatter"
           |        format = "$format"
           |        reformat = "$reformat"
           |        operation = reformat
           |      }
        """.stripMargin
      )

      Given("column to parse")
      import spark.implicits._
      val df = List("31/12/16 21:07:11", "01/12/16 17:07:11").toDF("date")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be change to date when correct")
      dfResult.select("date").collect.map(_.getAs[String]("date")).toSet shouldBe Set("21", "17")

    }

    scenario("check parse date fail") {
      Given("config")

      val format = "dd/MM/yy"

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "date"
           |        type = "dateformatter"
           |        format = $format
           |      }
        """.stripMargin
      )

      Given("column to parse")
      import spark.implicits._
      val df = List("010117", "01 ENE 2017", "01 01 17").toDF("date")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)

      Then("text should be change to date when correct")
      dfResult.select("date").collect.map(_.getAs[Date]("date")).toSet shouldBe Set(None.orNull, None.orNull, None.orNull)

    }
  }

}

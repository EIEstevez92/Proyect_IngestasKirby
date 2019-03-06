package com.datio.kirby.api.implicits

import java.text.SimpleDateFormat
import java.util.Date

import com.datio.kirby.testUtils.InitSparkSessionFeature
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class ApplyFormatTest extends InitSparkSessionFeature with ApplyFormat with GivenWhenThen with Matchers {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  feature("Apply type Date to Data Frames follow the schema") {

    scenario("Date in long") {

      Given("A dataFrame with long elements")
      import spark.implicits._
      val df = List(123567890L, 2345678901L).toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DATE").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDate(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("1973-12-01 00:00:00.000", "2044-05-01 00:00:00.000")
    }

    scenario("Date in string with format yyyy-MM-dd") {

      Given("A dataFrame with string elements")
      import spark.implicits._
      val df = List("2017-04-26", "2017-12-31").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DATE(10)").putString("format", "yyyy-MM-dd").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDate(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2017-04-26 00:00:00.000", "2017-12-31 00:00:00.000")
    }

    scenario("Date in string with format dd-MMM-yy and locale us") {

      Given("A dataFrame with string elements")
      import spark.implicits._
      val df = List("13-NOV-15", "21-JAN-16", "11-OCT-16").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DATE(9)").putString("format", "dd-MMM-yy").putString("locale", "us").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDate(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2015-11-13 00:00:00.000", "2016-01-21 00:00:00.000", "2016-10-11 00:00:00.000")
    }

    scenario("Date in string with format dd-MMM-yy and locale us_US") {

      Given("A dataFrame with string elements")
      import spark.implicits._
      val df = List("13-NOV-15", "21-JAN-16", "11-OCT-16").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DATE").putString("format", "dd-MMM-yy").putString("locale", "us_US").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDate(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2015-11-13 00:00:00.000", "2016-01-21 00:00:00.000", "2016-10-11 00:00:00.000")
    }

    scenario("Date invalid") {

      Given("A dataFrame with invalid elements")
      import spark.implicits._
      val df = List("17-04-ab", "20174231").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DATE").putString("format", "yyyy-MM-dd").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDate(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).toSet shouldBe Set(None.orNull, None.orNull)
    }
  }

  feature("Apply type Timestamp to Data Frames follow the schema") {

    scenario("Timestamp in long") {

      Given("A dataFrame with a long element")
      import spark.implicits._
      val df = List(123567890L, 2345678901L).toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("1973-12-01 05:24:50.000", "2044-05-01 03:28:21.000")
    }

    scenario("Timestamp in string yyyy-MM-dd") {

      Given("A dataFrame with a long element")
      import spark.implicits._
      val df = List("2017-04-26", "2017-12-31").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP(10)").putString("format", "yyyy-MM-dd").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2017-04-26 00:00:00.000", "2017-12-31 00:00:00.000")
    }

    scenario("Timestamp in string with format dd-MMM-yy and locale en_US") {

      Given("A dataFrame with string elements")
      import spark.implicits._
      val df = List("13-NOV-15", "21-JAN-16", "11-OCT-16").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP").putString("format", "dd-MMM-yy").putString("locale", "en_US").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2015-11-13 00:00:00.000", "2016-01-21 00:00:00.000", "2016-10-11 00:00:00.000")
    }

    scenario("Timestamp in string yyyy-MM-dd HH:mm:ss") {

      Given("A dataFrame with a long element")
      import spark.implicits._
      val df = List("2017-04-26 11:59:59", "2017-12-31 23:59:59").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP(19)").putString("format", "yyyy-MM-dd HH:mm:ss").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2017-04-26 11:59:59.000", "2017-12-31 23:59:59.000")
    }

    scenario("Timestamp in string dd/MM/yyyy HH:mm:ss.SSSSSSSSS") {

      Given("A dataFrame with a long element")
      import spark.implicits._
      val df = List("26/04/2017 11:59:59.999999999", "31/12/2017 23:59:59.999999999").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP").putString("format", "dd/MM/yyyy HH:mm:ss.SSSSSSSSS").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2017-04-26 11:59:59.999", "2017-12-31 23:59:59.999")
    }

    scenario("Timestamp in string dd/MM/yyyy HH:mm:ss.SSSSSS") {

      Given("A dataFrame with a long element")
      import spark.implicits._
      val df = List("26/04/2017 11:59:59.999999", "31/12/2017 23:59:59.999999").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP").putString("format", "dd/MM/yyyy HH:mm:ss.SSSSSS").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2017-04-26 11:59:59.999", "2017-12-31 23:59:59.999")
    }

    scenario("Timestamp in string dd/MM/yyyy HH:mm:ss.n") {

      Given("A dataFrame with a long element")
      import spark.implicits._
      val df = List("26/04/2017 11:59:59.999999999", "31/12/2017 23:59:59.999999999").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP").putString("format", "dd/MM/yyyy HH:mm:ss.n").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).map(sdf.format).toSet shouldBe Set("2017-04-26 11:59:59.999", "2017-12-31 23:59:59.999")
    }

    scenario("Timestamp invalid") {

      Given("A dataFrame with date invalid element")
      import spark.implicits._
      val df = List("17-04-ab", "20174231").toDF("date")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("date", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "TIMESTAMP").putString("format", "yyyy-MM-dd").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeTimestamp(schema)

      Then("Column casted to date")
      dfFormatted.collect().map(_.getAs[Date]("date")).toSet shouldBe Set(None.orNull, None.orNull)
    }
  }

  feature("Apply type Number to Data Frames follow the schema") {

    scenario("Valid Number") {
      Given("A dataFrame with number in ES")
      import spark.implicits._
      val df = List("38303.45", "123123.123").toDF("number")

      Given("A schema with number")
      val schema = StructType(List(StructField("number", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(38,12)").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("38303.450000000000", "123123.123000000000")
    }

    scenario("Invalid Number") {
      Given("A dataFrame with number in ES")
      import spark.implicits._
      val df = List("12asd345", "123123,123d").toDF("number")

      Given("A schema with number")
      val schema = StructType(List(StructField("number", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(12,12)").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number")).toSet shouldBe Set(None.orNull)
    }

    scenario("Number with other type (valid cast)") {
      Given("A dataFrame with number in ES")
      import spark.implicits._
      val df = List(123.123d, 123123.123d).toDF("number")

      Given("A schema with number")
      val schema = StructType(List(StructField("number", StringType, nullable = false,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(38,12)").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("123.123000000000", "123123.123000000000")
    }

    scenario("Number with locale 'es'") {
      Given("A dataFrame with number in ES")
      import spark.implicits._
      val df = List("4.294.967.295,000").toDF("number")

      Given("A schema with number and locale")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(38,6)").putString("locale", "es").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("4294967295.000000")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(38, 6)
    }

    scenario("Number with locale 'es_ES'") {
      Given("A dataFrame with number in es_ES")
      import spark.implicits._
      val df = List("4.294.967.295,000").toDF("number")

      Given("A schema with number and locale")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(38,3)").putString("locale", "es_ES").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("4294967295.000")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(38, 3)
    }

    scenario("Number with locale 'us_US'") {
      Given("A dataFrame with number in us_US")
      import spark.implicits._
      val df = List("4,294,967,295.00").toDF("number")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(22,6)(18)").putString("locale", "us_US").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("4294967295.000000")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(22, 6)
    }

    scenario("Number with locale fixed unsigned") {
      Given("A dataFrame with number in fixed unsigned")
      import spark.implicits._
      val df = List("000082300").toDF("number")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(22,4)(18)").
          putString("locale", "fixed_unsigned").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("8.2300")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(22, 4)
    }

    scenario("Number with locale fixed left signed") {
      Given("A dataFrame with number in fixed left signed")
      import spark.implicits._
      val df = List("+000082300").toDF("number")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(22,4)(18)").
          putString("locale", "fixed_signed_left").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("8.2300")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(22, 4)
    }

    scenario("Number with locale fixed negative left signed") {
      Given("A dataFrame with number in fixed negative left signed")
      import spark.implicits._
      val df = List("-000082300").toDF("number")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(22,4)(18)").
          putString("locale", "fixed_signed_left").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("-8.2300")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(22, 4)
    }

    scenario("Number with locale fixed right signed") {
      Given("A dataFrame with number in right signed")
      import spark.implicits._
      val df = List("000082300+").toDF("number")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(22,4)(18)").
          putString("locale", "fixed_signed_right").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("8.2300")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(22, 4)
    }

    scenario("Number with locale fixed negative right signed") {
      Given("A dataFrame with number in negative right signed")
      import spark.implicits._
      val df = List("000082300-").toDF("number")

      Given("A schema with date and format")
      val schema = StructType(List(StructField("number", StringType, nullable = true,
        new MetadataBuilder().putString("logicalFormat", "DECIMAL(22,4)(18)").
          putString("locale", "fixed_signed_right").build())))

      When("Apply format")
      val dfFormatted = df.castOriginTypeDecimal(schema)

      Then("Column casted to big decimal")
      dfFormatted.collect().map(_.getAs[java.math.BigDecimal]("number").toString).toSet shouldBe Set("-8.2300")
      dfFormatted.schema.filter(_.name == "number").map(_.dataType).head shouldBe DecimalType(22, 4)
    }
  }
}


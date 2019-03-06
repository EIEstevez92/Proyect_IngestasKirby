package com.datio.kirby.util

import com.datio.kirby.api.exceptions.KirbyException
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class ConversionTypeUtilTest extends FeatureSpec with GivenWhenThen with Matchers {

  val conversionTypeUtil = new ConversionTypeUtil {}
  val validCatalog = Seq(
    ("string", StringType),
    (" StRIng", StringType),
    ("int32", IntegerType),
    (" InT32  ", IntegerType),
    ("int", IntegerType),
    (" Int", IntegerType),
    ("integer", IntegerType),
    ("int64", LongType),
    ("   INT64", LongType),
    ("Long   ", LongType),
    ("double", DoubleType),
    ("  Double ", DoubleType),
    ("  Double ", DoubleType),
    ("float", FloatType),
    ("    FLOAT", FloatType),
    ("boolean", BooleanType),
    ("  BooLean", BooleanType),
    ("date", DateType),
    ("   dAte   ", DateType),
    ("timestamp_millis", TimestampType),
    ("   Timestamp_Millis", TimestampType),
    ("decimal(12)", DecimalType(12, 0)),
    ("  Decimal (    14 ) ", DecimalType(14, 0)),
    ("decimal(23, 1)", DecimalType(23, 1)),
    ("   deCIMal  (   26  ,    012 ) ", DecimalType(26, 12)),
    ("decimal (23,11)  ", DecimalType(23, 11))
  )

  val invalidCatalog = Seq("sttring", "decimal()", "unknown")

  feature("data type from a value") {

    scenario("get its proper data type") {
      validCatalog.foreach(t => {
        Given(s"""a datatype with value ${t._1}""")

        When("getting its data type")
        val dataType = conversionTypeUtil.typeToCast(t._1)

        Then("should return its proper data type")
        dataType should be(t._2)
      })
    }

    scenario("get an error when getting the data type") {
      invalidCatalog.foreach(t => {
        Given(s"""an invalid datatype with value ${t}""")

        When("getting its data type")

        Then("should throw an exception")
        assertThrows[KirbyException] {
          conversionTypeUtil.typeToCast(t)
        }
      })
    }

  }

}
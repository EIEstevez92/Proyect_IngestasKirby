package com.datio.kirby.transformation.column

import com.datio.kirby.config.TransformationFactory
import com.datio.kirby.constants.CommonLiterals._
import com.datio.kirby.testUtils.InitSparkSessionFeature
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkException
import org.apache.spark.sql.Column
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{GivenWhenThen, Matchers}

@RunWith(classOf[JUnitRunner])
class TokenizerTest extends InitSparkSessionFeature with GivenWhenThen with Matchers with TransformationFactory {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val url = getClass.getResource("/caas/caas-cloud.cfg")
    spark.sparkContext.addFile(url.getPath)
  }

  feature("Check tokenizer transformations") {
    import spark.implicits._
    scenario("encrypt with cclient") {
      Given("config")

      val typeEncrypt = CLIENT_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("aa", "aa"), ("bb", "bb"), ("cc", "cc")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with email") {
      Given("config")

      val typeEncrypt = MAIL_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("email@bbva.es", "email@bbva.es"), ("email@datiobd.es", "email@datiobd.es")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with nif") {
      Given("config")

      val typeEncrypt = NIF_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("00112233a", "00112233a"), ("99999999z", "99999999z")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with phone") {
      Given("config")

      val typeEncrypt = PHONE_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("123456789", "123456789"), ("00000000", "00000000")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with codicontra") {
      Given("config")

      val typeEncrypt = COD_IC_CONTRA_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("aa", "aa"), ("bb", "bb"), ("cc", "cc")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with alphanumeric") {
      Given("config")

      val typeEncrypt = ALPHANUMERIC_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("aa3", "aa3"), ("b2b", "b2b"), ("1cc", "1cc")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with alphanumeric_extended") {
      Given("config")

      val typeEncrypt = "alphanumeric_extended"
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("a_a3", "a_a3"), ("b2/b", "b2/b"), ("1c_c", "1c_c")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with numeric") {
      Given("config")

      val typeEncrypt = NUMERIC_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("14", "14"), ("257", "257"), ("789", "789")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with numeric_extended") {
      Given("config")

      val typeEncrypt = NUMERIC_EXT_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("/14", "/14"), ("25_7", "25_7"), ("78/9", "78/9")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with pan") {
      Given("config")

      val typeEncrypt = PAN_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("123456789123", "123456789123"), ("558448487558", "558448487558"), ("843167696332", "843167696332")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), typeEncrypt))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with pan type error Mask") {
      Given("config")

      val typeEncrypt = PAN_TK
      val invalidTypeValue = "aa"
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |        mode = "MASK_FAILED"
           |        invalidTypeValue = "$invalidTypeValue"
           |      }
        """.stripMargin)

      Given("column to parse")

      import spark.implicits._
      val df = List("aaabbbcccc", "adasd", "123213231v").toDF("tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .collect()
          .forall(
            row =>
              row.getAs[String]("tokenText") == invalidTypeValue
          )
      )
    }

    scenario("encrypt with pan library error Mask") {
      Given("config")

      val typeEncrypt = PAN_TK
      val invalidTokenizationValue = "aa"
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |        mode = "MASK_FAILED"
           |        invalidTokenizationValue = "$invalidTokenizationValue"
           |      }
        """.stripMargin)

      Given("column to parse")

      import spark.implicits._
      val df = List("111", "1", "112").toDF("tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .collect()
          .forall(
            row =>
              row.getAs[String]("tokenText") == invalidTokenizationValue
          )
      )
    }

    scenario("encrypt with pan error failFast") {
      Given("config")

      val typeEncrypt = PAN_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |        mode = "FAIL_FAST"
           |      }
        """.stripMargin)

      Given("column to parse")

      import spark.implicits._
      val df = List(("123456789123", "123456789123"), ("558448487558", "aaabbbcccc"), ("843167696332", "843167696332")).toDF("originalText", "tokenText")

      When("apply transformations")

      val caught = intercept[SparkException] {
        readTransformation(config)(spark).transform(df).collect()
      }
      assert(caught.getCause.getMessage === "The value of aaabbbcccc is not valid value for pan tokenization. It should be a numeric value with 11-19 characters.")
    }

    scenario("encrypt with date_extended") {
      Given("config")
      val typeEncrypt = DATE_EXT_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |        formatDate = "yyyy-MM-dd"
           |      }
        """.stripMargin)

      Given("column to parse")
      import spark.implicits._
      val df = List(("2017-05-08", "2017-05-08"), ("2017-10-14", "2017-10-14"), ("2015-03-25", "2015-03-25")).toDF("originalText", "tokenText")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)
      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), "date_extended3"))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with date_extended with format dd-MM-yyyy") {
      Given("config")
      val typeEncrypt = DATE_EXT_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |        formatDate = "dd-MM-yyyy"
           |      }
        """.stripMargin)

      Given("column to parse")
      import spark.implicits._
      val df = List(("08-05-2017", "08-05-2017"), ("14-10-2017", "14-10-2017"), ("25-03-2015", "25-03-2015")).toDF("originalText", "tokenText")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)
      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), "date_extended2"))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt with date in default format") {
      Given("config")
      val typeEncrypt = DATE_EXT_TK
      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |        typeEncrypt = "$typeEncrypt"
           |      }
        """.stripMargin)

      Given("column to parse")
      import spark.implicits._
      val df = List(("08/05/2017", "08/05/2017"), ("14/10/2017", "14/10/2017"), ("25/03/2015", "25/03/2015")).toDF("originalText", "tokenText")

      When("apply transformations")
      val dfResult = readTransformation(config)(spark).transform(df)
      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), DATE_EXT_TK))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }

    scenario("encrypt by default cclient") {
      Given("config")

      val config = ConfigFactory.parseString(
        s"""
           |      {
           |        field = "tokenText"
           |        type = "token"
           |      }
        """.stripMargin)

      Given("column to parse")

      val df = List(("aa", "aa"), ("bb", "bb"), ("cc", "cc")).toDF("originalText", "tokenText")

      When("apply transformations")

      val dfResult = readTransformation(config)(spark).transform(df)

      assert(
        dfResult
          .withColumn("textDecrypt", undoTokenization(dfResult("tokenText"), "cclient"))
          .collect()
          .forall(
            row =>
              row.getAs[String]("originalText") == row.getAs[String]("textDecrypt")
          )
      )
    }
  }

  private def undoTokenization(col: Column, typeEncrypt: String): Column = {

    import com.datio.tokenization.functions.DecryptFunctions._
    typeEncrypt.toLowerCase match {
      case CLIENT_TK => decryptCclient(col)
      case NIF_TK => decryptNif(col)
      case MAIL_TK => decryptMail(col)
      case PHONE_TK => decryptPhone(col)
      case COD_IC_CONTRA_TK => decryptCodIcontra(col)
      case PAN_TK => decryptPan(col)
      case ALPHANUMERIC_TK => decryptAlphanumeric(col)
      case ALPHANUMERIC_EXT_TK => decryptAlphanumericExtended(col)
      case NUMERIC_TK => decryptNumeric(col)
      case NUMERIC_EXT_TK => decryptNumericExtended(col)
      case DATE_EXT_TK => decryptDate("dd/MM/yyyy")(col)
      case "date_extended2" => decryptDate("dd-MM-yyyy")(col)
      case "date_extended3" => decryptDate("yyyy-MM-dd")(col)
    }
  }
}

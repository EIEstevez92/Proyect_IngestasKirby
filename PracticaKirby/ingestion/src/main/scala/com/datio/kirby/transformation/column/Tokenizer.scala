package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.config.configurable
import com.datio.kirby.constants.CommonLiterals._
import com.datio.kirby.errors.TRANSFORMATION_TOKENIZER_MODE_ERROR
import com.datio.tokenization.functions.EncryptFunctions
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, SparkSession}

import scala.util.Try

/**
  * Tokenize field
  *
  * @param config config for tokenizer.
  * @param spark  spark session object.
  */
@configurable("token")
class Tokenizer(val config: Config)(implicit val spark: SparkSession) extends ColumnTransformation {

  private val tokenizerModeFailFast = "FAIL_FAST"
  private val tokenizerModeMaskFailed = "MASK_FAILED"

  private val typeEncrypt = Try(config.getString("typeEncrypt")).getOrElse("cclient")
  private lazy val mode = Try(config.getString("mode")).getOrElse(tokenizerModeMaskFailed)
  private lazy val invalidTypeValue = Try(config.getString("invalidTypeValue")).getOrElse("0000000000000000")
  private lazy val invalidTokenizationValue = Try(config.getString("invalidTokenizationValue")).getOrElse("9999999999999999")

  import EncryptFunctions._

  /**
    * Method transform column.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  override def transform(col: Column): Column = {
    typeEncrypt.toLowerCase match {
      case CLIENT_TK => encryptCclient(col)
      case NIF_TK => encryptNif(col)
      case MAIL_TK => encryptMail(col)
      case PHONE_TK => encryptPhone(col)
      case COD_IC_CONTRA_TK => encryptCodIcontra(col)
      case PAN_TK => panTokenization(col)
      case ALPHANUMERIC_TK => encryptAlphanumeric(col)
      case ALPHANUMERIC_EXT_TK => encryptAlphanumericExtended(col)
      case NUMERIC_TK => encryptNumeric(col)
      case NUMERIC_EXT_TK => encryptNumericExtended(col)
      case DATE_EXT_TK =>
        val pattern = Try(config.getString("formatDate")).getOrElse("dd/MM/yyyy")
        encryptDate(pattern)(col)
      case name: String => encryptGeneric(name)(col)
    }
  }

  private def panTokenization(col: Column) = {
    mode toUpperCase match {
      case `tokenizerModeFailFast` | `tokenizerModeMaskFailed` =>
      case _ => throw new KirbyException(
        TRANSFORMATION_TOKENIZER_MODE_ERROR,
        mode,
        List(tokenizerModeFailFast, tokenizerModeMaskFailed).mkString(", ")
      )
    }
    encryptPan(mode, invalidTypeValue, invalidTokenizationValue)(col)
  }
}

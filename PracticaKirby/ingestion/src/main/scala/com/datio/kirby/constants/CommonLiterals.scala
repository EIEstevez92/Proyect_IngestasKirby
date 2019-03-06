package com.datio.kirby.constants

/**
  * Literals needed in the code
  * */
object CommonLiterals {

  val DASH = " - "

  //Tokenization fields
  val CLIENT_TK = "cclient"
  val NIF_TK = "nif"
  val MAIL_TK = "mail"
  val PHONE_TK = "phone"
  val COD_IC_CONTRA_TK = "codicontra"
  val PAN_TK = "pan"
  val ALPHANUMERIC_TK = "alphanumeric"
  val ALPHANUMERIC_EXT_TK = "alphanumeric_extended"
  val NUMERIC_TK = "numeric"
  val NUMERIC_EXT_TK = "numeric_extended"
  val DATE_EXT_TK = "date_extended"

  //Tokenization encrypt UDFs
  val CLIENT_ENC = "encryptCclient"
  val NIF_ENC = "encryptNif"
  val MAIL_ENC = "encryptMail"
  val PHONE_ENC = "encryptPhone"
  val COD_IC_CONTRA_ENC = "encryptCodIcontra"
  val PAN_ENC = "encryptPan"
  val ALPHANUMERIC_ENC = "encryptAlphanumeric"
  val ALPHANUMERIC_EXT_ENC = "encryptAlphanumericExtended"
  val NUMERIC_ENC = "encryptNumeric"
  val NUMERIC_EXT_ENC = "encryptNumericExtended"
  val DATE_EXT_ENC = "encryptDateExtended"

  //Tokenization decrypt UDFs
  val CLIENT_DEC = "decryptCclient"
  val NIF_DEC = "decryptNif"
  val MAIL_DEC = "decryptMail"
  val PHONE_DEC = "decryptPhone"
  val COD_IC_CONTRA_DEC = "decryptCodIcontra"
  val PAN_DEC = "decryptPan"
  val ALPHANUMERIC_DEC = "decryptAlphanumeric"
  val ALPHANUMERIC_EXT_DEC = "decryptAlphanumericExtended"
  val NUMERIC_DEC = "decryptNumeric"
  val NUMERIC_EXT_DEC = "decryptNumericExtended"
  val DATE_EXT_DEC = "decryptDateExtended"
}

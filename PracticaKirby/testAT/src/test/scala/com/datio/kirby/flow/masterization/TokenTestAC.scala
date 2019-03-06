package com.datio.kirby.flow.masterization


import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.{FunSuite, Matchers}


object TokenTestAC extends FunSuite with Matchers {

  // scalastyle:off method.length
  def check(dfResult: DataFrame): Unit = {

    val url = getClass.getResource("/caas/caas-cloud.cfg")
    dfResult.sqlContext.sparkContext.addFile(url.getPath)
    sys.props.put("tokenization_app_name", "cloud")
    sys.props.put("tokenization_app_pass", "eG73ao2adAkZQl1PmYg")
    sys.props.put("tokenization_app_endpoint", "LOCAL")


    val dfResultWithUndos =
      dfResult
        .withColumn("decryptTokenDefault", undoTokenization(dfResult("tokendefault"), "cclient"))
        .withColumn("decryptTokenCclient", undoTokenization(dfResult("tokencclient"), "cclient"))
        .withColumn("decryptTokenNif", undoTokenization(dfResult("tokennif"), "nif"))
        .withColumn("decryptTokenMail", undoTokenization(dfResult("tokenmail"), "mail"))
        .withColumn("decryptTokenPhone", undoTokenization(dfResult("tokenphone"), "phone"))
        .withColumn("decryptTokenCodIContra", undoTokenization(dfResult("tokencodicontra"), "codicontra"))
        .withColumn("decryptTokenAlphanumeric", undoTokenization(dfResult("tokenalphanumeric"), "alphanumeric"))
        .withColumn("decryptTokenAlphanumericExtended", undoTokenization(dfResult("tokenalphanumeric_extended"), "alphanumeric_extended"))
        .withColumn("decryptTokenNumeric", undoTokenization(dfResult("tokennumeric"), "numeric"))
        .withColumn("decryptTokenNumericExtended", undoTokenization(dfResult("tokennumeric_extended"), "numeric_extended"))
        .withColumn("decryptTokenPan", undoTokenization(dfResult("tokenpan"), "pan"))
        .withColumn("decryptTokenDateExtended", undoTokenization(dfResult("tokendate_extended"), "date_extended"))
        .collect()

    dfResultWithUndos.length shouldBe 7

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenDefault") === "tokendefault"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenCclient") === "tokencclient"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenNif") === "tokennif"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenMail") === "tokenmail"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenPhone") === "123456"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenCodIContra") === "tokencodicontra"
        )
    )
    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenAlphanumeric") === "hhdfghdf123"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenAlphanumericExtended") === "hhdfghd_f123"
        )
    )
    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenNumeric") === "1234"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenNumericExtended") === "123_4"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenPan") === "123456789123"
        )
    )

    assert(
      dfResultWithUndos
        .forall(row =>
          row.getAs[String]("decryptTokenDateExtended") === "2017/05/08"
        )
    )

  }

  private def undoTokenization(col: Column, typeEncrypt: String): Column = {

    import com.datio.tokenization.functions.DecryptFunctions._
    typeEncrypt.toLowerCase match {
      case "cclient" => decryptCclient(col)
      case "nif" => decryptNif(col)
      case "mail" => decryptMail(col)
      case "phone" => decryptPhone(col)
      case "codicontra" => decryptCodIcontra(col)
      case "pan" => decryptPan(col)
      case "alphanumeric" => decryptAlphanumeric(col)
      case "alphanumeric_extended" => decryptAlphanumericExtended(col)
      case "numeric" => decryptNumeric(col)
      case "numeric_extended" => decryptNumericExtended(col)
      case "date_extended" => decryptDate("yyyy/MM/dd")(col)
    }
  }
}

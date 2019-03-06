package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object IntegrityTestAC extends FunSuite with Matchers{

  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
      .select("allIntegrityOK")
      .collect()
      .forall(
        _.getAs[String]("allIntegrityOK") === "key1,value1"
      )
    )

    assert(
      dfResult
        .select("allIntegrityFail")
        .collect()
        .forall(
          _.getAs[String]("allIntegrityFail") === "defaultIntegrity"
        )
    )

  }
}

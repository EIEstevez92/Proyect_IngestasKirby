package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object PartialInfoTestAC extends FunSuite with Matchers{

  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
      .select("importantDataFromFieldReference")
      .collect()
      .forall(
        _.getAs[String]("importantDataFromFieldReference") === "importantData"
      )
    )

  }

}

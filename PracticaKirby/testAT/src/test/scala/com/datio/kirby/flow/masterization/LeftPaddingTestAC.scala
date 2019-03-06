package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object LeftPaddingTestAC extends FunSuite with Matchers{

  def check (dfResult : DataFrame) : Unit = {

    assert(
      dfResult
      .select("office1character")
      .collect()
      .forall(
        _.getAs[String]("office1character") === "0003"
      )
    )

    assert(
      dfResult
        .select("office2character")
        .collect()
        .forall(
          _.getAs[String]("office2character") === "0013"
        )
    )


    assert(
      dfResult
        .select("office3character")
        .collect()
        .forall(
          _.getAs[String]("office3character") === "0213"
        )
    )


    assert(
      dfResult
        .select("office4character")
        .collect()
        .forall(
          _.getAs[String]("office4character") === "2213"
        )
    )

  }

}

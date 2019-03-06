package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object MaskTestAC extends FunSuite with Matchers{

  // scalastyle:off method.length
  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
      .select("maskstring")
      .collect()
      .forall(
        _.getAs[String]("maskstring") === "X"
      )
    )

    assert(
      dfResult
        .select("maskinteger")
        .collect()
        .forall(
          _.getAs[Int]("maskinteger") === 0
        )
    )

    assert(
      dfResult
        .select("maskdouble")
        .collect()
        .forall(
          _.getAs[Double]("maskdouble") === 0D
        )
    )

    assert(
      dfResult
        .select("masklong")
        .collect()
        .forall(
          _.getAs[Long]("masklong") === 0L
        )
    )

    assert(
      dfResult
        .select("maskfloat")
        .collect()
        .forall(
          _.getAs[Float]("maskfloat") === 0F
        )
    )

    assert(
      dfResult
        .select("maskother")
        .collect()
        .forall(
          _.getAs[String]("maskother") === "XX"
        )
    )

  }
  // scalastyle:on method.length

}

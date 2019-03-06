package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object LiteralTestAC extends FunSuite with Matchers{

  // scalastyle:off method.length
  def check (dfResult : DataFrame) : Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
      .select("literalDefault")
      .collect()
      .forall(
        _.getAs[String]("literalDefault") === "literalDefault"
      )
    )

    assert(
      dfResult
        .select("literalString")
        .collect()
        .forall(
          _.getAs[String]("literalString") === "string"
        )
    )

    assert(
      dfResult
        .select("literalInt32")
        .collect()
        .forall(
          _.getAs[Int]("literalInt32") === 1
        )
    )

    assert(
      dfResult
        .select("literalInt")
        .collect()
        .forall(
          _.getAs[Int]("literalInt") === 2
        )
    )

    assert(
      dfResult
        .select("literalInteger")
        .collect()
        .forall(
          _.getAs[Int]("literalInteger") === 3
        )
    )

    assert(
      dfResult
        .select("literalInt64")
        .collect()
        .forall(
          _.getAs[Long]("literalInt64") === 1L
        )
    )

    assert(
      dfResult
        .select("literalLong")
        .collect()
        .forall(
          _.getAs[Long]("literalLong") === 2L
        )
    )

    assert(
      dfResult
        .select("literalDouble")
        .collect()
        .forall(
          _.getAs[Double]("literalDouble") === 1D
        )
    )

    assert(
      dfResult
        .select("literalFloat")
        .collect()
        .forall(
          _.getAs[Float]("literalFloat") === 1F
        )
    )

    assert(
      dfResult
        .select("literalBoolean")
        .collect()
        .forall(
          _.getAs[Boolean]("literalBoolean") === true
        )
    )

    assert(
      dfResult
        .select("literalDecimalPrecisionScale")
        .collect()
        .forall(
          _.getAs[java.math.BigDecimal]("literalDecimalPrecisionScale") === new java.math.BigDecimal("125.52")
        )
    )

    assert(
      dfResult
        .select("literalDecimalPrecision")
        .collect()
        .forall(
          _.getAs[java.math.BigDecimal]("literalDecimalPrecision") === new java.math.BigDecimal("126")
        )
    )

  }
  // scalastyle:on method.length

}

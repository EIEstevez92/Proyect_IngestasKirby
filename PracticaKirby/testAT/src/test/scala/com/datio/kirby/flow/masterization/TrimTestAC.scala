package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object TrimTestAC extends FunSuite with Matchers {

  def check (dfResult : DataFrame) : Unit = {

    assert(
      dfResult
        .select("leftTrim")
        .collect()
        .forall { x =>
          x.getAs[String]("leftTrim") === "test of trim "
        }
    )

    assert(
      dfResult
        .select("bothTrim")
        .collect()
        .forall(
          _.getAs[String]("bothTrim") === "test of trim"
        )
    )

    assert(
      dfResult
        .select("rightTrim")
        .collect()
        .forall(
          _.getAs[String]("rightTrim") === " test of trim"
        )
    )

    assert(
      dfResult
        .select("defaultTrim")
        .collect()
        .forall(
          _.getAs[String]("defaultTrim") === "test of trim"
        )
    )

  }

}

package com.datio.kirby.flow.masterization

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSuite, Matchers}


object CatalogTestAC extends FunSuite with Matchers {

  def check(dfResult: DataFrame): Unit = {

    dfResult.count() shouldBe 7

    assert(
      dfResult
        .select("allValuesInCatalog")
        .collect()
        .forall(
          _.getAs[String]("allValuesInCatalog") === "value1"
        )
    )

    assert(
      dfResult
        .select("NoValuesInCatalog")
        .collect()
        .forall(
          _.getAs[String]("NoValuesInCatalog") === null
        )
    )
  }

}

package com.datio.kirby.errors

import com.datio.kirby.errors.ObtainErrorList._
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KirbyErrorListTest extends InitSparkSessionFeature with FileTestUtil with GivenWhenThen with Matchers {

  feature("Kirby errors must have unique codes") {

    scenario("Current kirby compilation") {

      Given("An array with the kirby errors declared in packages com.datio.kirby.api.errors and com.datio.kirby.errors")

      val allErrors = getAllKirbyErrorsFromPackageObject()

      When("Extract the codes and gets duplicates")
      val duplicatedCodes = allErrors.map(ke => ke.code).groupBy(identity).filter{case (_, list) => list.length > 1}.keys


      Then("Duplicates list is empty")
      assert(duplicatedCodes.size == 0)
    }
  }
}

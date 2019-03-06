package com.datio.kirby.errors

import com.datio.kirby.api.errors.KirbyError
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KirbyErrorTest extends InitSparkSessionFeature with FileTestUtil with GivenWhenThen with Matchers {

  feature("Create a correct Kirby Error") {

    scenario("Create a KirbyError from a code and message") {

      Given("A numeric code and a string message")
      val code = 1
      val message = "Error message 1"


      When("Create a KirbyError from they")
      val kError = KirbyError(code, message)

      Then("KirbyError methods have to be correct")
      assert(kError.code == code)
      assert(kError.message == message)
      assert(kError.toString() == "1 - Error message 1")
      assert(kError.toFormattedString() == "1 - Error message 1")
      assert(kError.messageToFormattedString() == "Error message 1")
    }

    scenario("Create a KirbyError from a code and message thats accepts params") {

      Given("A numeric code and a string message")
      val code = 1
      val message = "Error message 1 with param: %s"
      val param = "parameter 1"

      When("Create a KirbyError from they")
      val kError = KirbyError(code, message)

      Then("KirbyError methods have to be correct")
      assert(kError.code == code)
      assert(kError.message == message)
      assert(kError.toString() == "1 - Error message 1 with param: %s")
      assert(kError.toFormattedString(param) == "1 - Error message 1 with param: parameter 1")
      assert(kError.messageToFormattedString(param) == "Error message 1 with param: parameter 1")
    }
  }
}

package com.datio.kirby.api.exceptions

import com.datio.kirby.api.errors.KirbyError
import com.datio.kirby.testUtils.{FileTestUtil, InitSparkSessionFeature}
import org.junit.runner.RunWith
import org.scalatest.{GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KirbyExceptionTest extends InitSparkSessionFeature with FileTestUtil with GivenWhenThen with Matchers {

  feature("Create a correct Kirby Exception") {

    scenario("Kirby Exception from a KirbyError") {

      Given("A Kirby Error")
      val kError = KirbyError(1, "Error 1")

      When("Create a Kirby Exception from it")
      val kExcept = new KirbyException(kError)

      Then("Exception message have to be correct")
      assert(kExcept.getMessage == kError.toFormattedString())
      assert(kError == kExcept.error)
    }

    scenario("Kirby Exception from a KirbyError with parameters") {

      val param = "param1"

      Given("A Kirby Error that accepts parameters")
      val kError = KirbyError(1, "Error 1 with param: %s")

      When("Create a Kirby Exception from it")
      val kExcept = new KirbyException(kError, param)

      Then("Exception message have to be correct")
      assert(kExcept.getMessage == kError.toFormattedString(param))
      assert(kError == kExcept.error)

    }

    scenario("Kirby Exception from a KirbyError and an Exception") {

      Given("A Kirby Error and an external exception")
      val kError = KirbyError(1, "Error 1")
      val exception = new RuntimeException("This is an external exception")

      When("Create a Kirby Exception from they")
      val kExcept = new KirbyException(kError, exception)

      Then("Exception message and cause have to be correct")
      assert(kExcept.getMessage == kError.toFormattedString())
      assert(kExcept.getCause == exception)
      assert(kError == kExcept.error)

    }

    scenario("Kirby Exception from a KirbyError and an Exception, with parameters") {

      val param = "param1"

      Given("A Kirby Error that accepts parameters and an external exception")
      val kError = KirbyError(1, "Error 1 with param: %s")
      val exception = new RuntimeException("This is an external exception")

      When("Create a Kirby Exception from they")
      val kExcept = new KirbyException(kError,exception, param)

      Then("Exception message have to be correct")
      assert(kExcept.getMessage == kError.toFormattedString(param))
      assert(kExcept.getCause == exception)
      assert(kError == kExcept.error)

    }

    scenario("Kirby Exception with custom message from a KirbyError") {

      Given("A Kirby Error and a message to throw")
      val kError = KirbyError(1, "Error 1")
      val errorMessage = "this is a custom error message"

      When("Create a Kirby Exception from they")
      val kExcept = new KirbyException(errorMessage, None.orNull, kError)

      Then("Exception message have to be correct")
      assert(kExcept.getMessage == errorMessage)
      assert(kExcept.getCause == None.orNull)
      assert(kError == kExcept.error)

    }

    scenario("Kirby Exception with custom message from a KirbyError and an external exception") {

      Given("A Kirby Error and a message to throw")
      val kError = KirbyError(1, "Error 1")
      val errorMessage = "this is a custom error message"
      val exception = new RuntimeException("This is an external exception")

      When("Create a Kirby Exception from they")
      val kExcept = new KirbyException(errorMessage, exception, kError)

      Then("Exception contents have to be correct")
      assert(kExcept.getMessage == errorMessage)
      assert(kExcept.getCause == exception)
      assert(kError == kExcept.error)

    }
  }
}

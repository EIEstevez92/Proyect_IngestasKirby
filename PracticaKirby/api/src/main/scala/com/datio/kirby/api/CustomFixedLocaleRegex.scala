package com.datio.kirby.api

import scala.util.matching.Regex

object CustomFixedLocaleRegex {
  val fixedSignedLeft = "fixed_signed_left"
  val fixedSignedLeftPattern: Regex = fixedSignedLeft.r
  val fixedSignedRight = "fixed_signed_right"
  val fixedSignedRightPattern: Regex = fixedSignedRight.r
  val fixedUnsigned = "fixed_unsigned"
  val fixedUnsignedPattern: Regex = fixedUnsigned.r
  val positiveSign = "+"
}

package com.datio.kirby.api.exceptions

import com.datio.kirby.api.errors.KirbyError


class KirbyException(message: String, throwable: Throwable = None.orNull, val error:KirbyError = None.orNull)
  extends Exception(message:String, throwable:Throwable) {

  def this(error:KirbyError, params:String*) = {
    this(error.toFormattedString(params: _*), None.orNull ,error)
  }

  def this(error:KirbyError, throwable: Throwable, params:String*) =
    this(error.toFormattedString(params:_*), throwable, error)
}

package com.datio.kirby

package object input {

  /**
    * Show the beginning and end field position in a String.
    *
    * @param initColumn character init data
    * @param endColumn  character end data
    */
  case class ColLong(initColumn: Int, endColumn: Int)

}

package com.datio.kirby.api

import org.apache.spark.sql.Row

package object implicits {

  implicit class RowFunctions(row: Row) extends Serializable {
    def cast(castFunctions: List[(Any) => Any]): Option[Any] = {
      val len = row.length
      var i = 0
      while (i < len) {
        if (!row.isNullAt(i)) {
          return Some(castFunctions(i)(row.get(i)))
        }
        i += 1
      }
      None
    }
  }

}

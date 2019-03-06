package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

@configurable("charactertrimmer")
class CharacterTrimmer(val config: Config) extends ColumnTransformation {

  override def transform(col: Column): Column = {

    val trimCharacter = Option(config.getString("charactertrimmer")).getOrElse("0")

    val method: String => String = x => x match {
      case _ if x.startsWith("+") => x.replaceFirst(s"^+$trimCharacter+", "")
      case _ if x.startsWith("-") => x.replaceFirst(s"^-$trimCharacter+", "-")
      case _ => x.replaceFirst(s"^$trimCharacter+", "")
    }

    val udfToApply = udf(method)
    udfToApply(col)

  }
}
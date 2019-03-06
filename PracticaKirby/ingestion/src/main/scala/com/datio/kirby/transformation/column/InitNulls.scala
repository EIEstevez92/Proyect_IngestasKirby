package com.datio.kirby.transformation.column

import com.datio.kirby.api.ColumnTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

/**
  * Initialize nulls to default value.
  *
  * @param config config for init null masterization.
  */
@configurable("initnulls")
class InitNulls(val config: Config) extends ColumnTransformation {

  /**
    * Custom transform to add udf.
    *
    * @param col to be transformed by masterization.
    * @return Column transformed.
    */
  override def transform(col: Column): Column = {

    // workaround to solve serializable task errors when count and log errors inside udf.
    val logSerializable = logger
    val defaultValueSerializable = config.getString("default")
    val fieldNameSerializable = columnName

    logSerializable.info("transform {} for column: {}", transformName, columnName)

    val method: Any => String = field =>

      if (Option(field).isEmpty) {
        logSerializable.debug("fail in transform initNulls -> set default in field: {}",
          fieldNameSerializable)

        defaultValueSerializable
      } else {
        field.toString
      }


    val udfToApply = udf(method)

    udfToApply(col)

  }

}

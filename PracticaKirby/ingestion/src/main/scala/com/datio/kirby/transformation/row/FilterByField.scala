package com.datio.kirby.transformation.row

import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.configurable
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.Try

object FilterByField {
  val FILTER_SUBCONFIG = "filters"
  val OPERATOR = "op"
  val FIELD = "field"
  val VALUE = "value"
  val LOGIC_OP = "logicOp"

  val EQ_OP = "eq"
  val NEQ_OP = "neq"
  val LT_OP = "lt"
  val LEQ_OP = "leq"
  val GT_OP = "gt"
  val GEQ_OP = "geq"
  val LIKE_OP = "like"
  val RLIKE_OP = "rlike"

  val AND_OP = "and"
  val OR_OP = "or"
}

/**
  * This class filter by custom criteria for a field.
  *
  * @param config filter by field.
  */
@configurable("filter,filterbyfield")
class FilterByField(val config: Config) extends RowTransformation {

  import FilterByField._

  private lazy val filters = config.getConfigList(FILTER_SUBCONFIG).asScala.toList

  private lazy val logicOperator = Try(config.getString(LOGIC_OP)).getOrElse(AND_OP).toLowerCase

  override def transform(df: DataFrame): DataFrame = {
    val conditionList = filters.map(cfg => {
      val rawFieldName = cfg.getString(FIELD).toColumnName
      val column = df.col(rawFieldName)
      val op = cfg.getString(OPERATOR)
      val value = cfg.getAnyRef(VALUE)

      logger.info(s"${getClass.getSimpleName}: applying filter $column $op $value")

      op.toLowerCase match {
        case EQ_OP => column.equalTo(value)
        case NEQ_OP => column.notEqual(value)
        case LT_OP => column.lt(value)
        case LEQ_OP => column.leq(value)
        case GT_OP => column.gt(value)
        case GEQ_OP => column.geq(value)
        case LIKE_OP => column.like(cfg.getString(VALUE))
        case RLIKE_OP => column.rlike(cfg.getString(VALUE))
        case _ => throw new IllegalArgumentException(s"[${getClass.getSimpleName}] 'op' is required")
      }
    })
    logicOperator match {
      case AND_OP => df.filter(conditionList.reduce((c1, c2) => c1 && c2))
      case OR_OP => df.filter(conditionList.reduce((c1, c2) => c1 || c2))
    }
  }

}
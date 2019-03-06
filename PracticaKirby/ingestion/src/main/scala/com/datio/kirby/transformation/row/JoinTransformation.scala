package com.datio.kirby.transformation.row

import com.datio.kirby.CheckFlow
import com.datio.kirby.api.RowTransformation
import com.datio.kirby.config.{InputFactory, SchemaReader, configurable}
import com.datio.kirby.constants.ConfigConstants.TRANSFORMATION_CONF_KEY
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

@configurable("join")
class JoinTransformation(val config: Config) extends RowTransformation
  with InputFactory with SchemaReader with CheckFlow {

  private val joins: Seq[Config] = Try(config.getConfigList("joins").asScala.toList) match {
    case Failure(t) => throw JoinTransformationException("JoinTransformation: 'joins' is mandatory", t)
    case Success(Nil) => throw JoinTransformationException("JoinTransformation: 'joins' cannot be empty")
    case Success(x) => x
  }

  val selfAlias = "self"

  private val select: Seq[String] = Try(config.getStringList("select").asScala).getOrElse(List("*"))

  private val resolveAmbiguityAuto: Boolean = Try(config.getBoolean("resolveConflictsAuto")).getOrElse(false)

  override def transform(startDf: DataFrame): DataFrame = {
    val extractedJoinConfig = extractJoinConfig(startDf)
    val joinResult = extractedJoinConfig.foldLeft(startDf.alias(selfAlias))((df, joinConfig) => {

      logger.debug(s"JoinTransformation: DF cols: ${df.columns.toList}")
      logger.debug(s"JoinTransformation: inputDf ${joinConfig.alias} cols: ${joinConfig.inputDf.columns.toList}")

      val inputDfWithAlias = joinConfig.inputDf.alias(joinConfig.alias)

      val joinExpression = Try(joinConfig.joinColumns.
        map(joinColumn => df.col(joinColumn.self) === inputDfWithAlias.col(joinColumn.other)).
        reduce(_ && _)
      ) match {
        case Failure(e) => throw JoinTransformationException("JoinTransformation: 'joinColumns' contains unknown columns", e)
        case Success(x) => x
      }
      logger.info(s"JoinTransformation: inputDfToJoin ${joinConfig.alias} type: ${joinConfig.joinType} join : $joinExpression")
      df.join(inputDfWithAlias, joinExpression, joinConfig.joinType)
    })

    val selectResult = Try(joinResult.selectExpr(select: _*)) match {
      case Failure(e) => throw JoinTransformationException("JoinTransformation: 'select' contains unknown columns", e)
      case Success(x) => x
    }

    val duplicatedColumns = getDuplicatedColumns(selectResult)

    if (duplicatedColumns.nonEmpty) {
      if (!resolveAmbiguityAuto) {
        throw JoinTransformationException(s"JoinTransformation: result contains ambiguous columns ${duplicatedColumns.mkString(", ")}")
      } else {
        val correctAmbiguity = resolveAmbiguity(extractedJoinConfig, selectResult, duplicatedColumns)
        val duplicatedColumnsInCorrected = getDuplicatedColumns(correctAmbiguity)
        if (duplicatedColumnsInCorrected.nonEmpty) {
          throw JoinTransformationException(s"JoinTransformation: There are duplicated columns in output " +
            s"that need to be resolved manually: ${duplicatedColumnsInCorrected.mkString(", ")}")
        }
        correctAmbiguity
      }
    } else {
      selectResult
    }
  }

  private def resolveAmbiguity(extractedJoinConfig: Seq[JoinConfig], selectResult: DataFrame, duplicatedColumns: Set[String]) = {
    selectResult.schema
    (for {
      joinConfig <- extractedJoinConfig
      column: String <- Try(selectResult.selectExpr(s"${joinConfig.alias}.*").columns).getOrElse(Array[String]())
      if duplicatedColumns.contains(column)
    } yield (joinConfig.alias, column)
      ).foldLeft(selectResult) { case (resolveConflictsDf, (alias, column)) =>
      resolveConflictsDf.
        withColumn(s"${alias}_$column", resolveConflictsDf.col(s"$alias.$column")).
        drop(resolveConflictsDf.col(s"$alias.$column"))
    }
  }

  def getDuplicatedColumns(df: DataFrame) = df.columns.toList.groupBy((k) => k).filter({ case (_, l) => l.size > 1 }).keySet

  private def extractJoinConfig(startDf: DataFrame): Seq[JoinConfig] = {
    val extractedJoinConfig = joins.map(joinConfig => {
      val joinType = getMandatory("joinType", joinConfig)
      val alias = getMandatory("alias", joinConfig)
      val joinColumns = getJoinColumns(joinConfig, alias)
      logger.debug(s"JoinTransformation: joinConfig $joinConfig")
      val inputDf = Try(joinConfig.getString("input")) match {
        case Success("self") => startDf
        case _ => Try(joinConfig.getConfig("input")) match {
          case Success(inputConfig) => getInput(startDf.sparkSession, alias, inputConfig)
          case _ => throw JoinTransformationException("JoinTransformation: 'input' is mandatory, and can be the string 'self' or an 'input' object")
        }
      }
      val transformations = Try(joinConfig.getConfigList(TRANSFORMATION_CONF_KEY).asScala).getOrElse(Seq())
        .map(transformationConfig => readTransformation(transformationConfig)(startDf.sparkSession))

      (alias, joinType, joinColumns, applyTransformations(inputDf, transformations)(startDf.sparkSession))
    })

    val aliases = extractedJoinConfig.map { case (alias, _, _, _) => alias }.toSet
    if (aliases.size < extractedJoinConfig.length) {
      throw JoinTransformationException("JoinTransformation: aliases must be unique")
    }
    if (aliases.contains(selfAlias)) {
      throw JoinTransformationException("JoinTransformation: alias name 'self' is reserved for main data frame")
    }

    extractedJoinConfig.map(joinConfig => {
      JoinConfig(joinConfig._1, joinConfig._2, joinConfig._3, joinConfig._4)
    })
  }

  protected def getInput(sparkSession: SparkSession, alias: String, inputConfig: Config) = {
    logger.debug(s"JoinTransformation: getting input from $alias")
    readInput(inputConfig).read(sparkSession)
  }

  private def getMandatory(key: String, config: Config): String = Try(config.getString(key)) match {
    case Failure(e) => throw JoinTransformationException(s"'$key' is mandatory ", e)
    case Success(x) => x
  }

  private def getJoinColumns(input: Config, alias: String): Seq[JoinColumn] = {
    Try(input.getConfigList("joinColumns").asScala.toList) match {
      case Failure(t) => throw JoinTransformationException("JoinTransformation: 'joinColumns' is mandatory", t)
      case Success(Nil) => throw JoinTransformationException("JoinTransformation: 'joinColumns' cannot be empty")
      case Success(joinColumnsConfig) => joinColumnsConfig.map(joinColumnConfig =>
        JoinColumn(getMandatory("self", joinColumnConfig).toColumnName, getMandatory("other", joinColumnConfig).toColumnName)
      )
    }
  }

  private case class JoinConfig(alias: String, joinType: String, joinColumns: Seq[JoinColumn], inputDf: DataFrame)

  private case class JoinColumn(self: String, other: String)

}

case class JoinTransformationException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)

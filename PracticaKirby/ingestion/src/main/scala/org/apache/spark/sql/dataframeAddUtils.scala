package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

/**
  * This class is needed because dropduplicates removes metadata from columns in dataframe.
  */
package object dataframeAddUtils {

  implicit class DatasetDropUtils(dataframe: DataFrame) {

    import dataframe._

    /**
      * (Scala-specific) Returns a new Dataset with duplicate rows removed, considering only
      * the subset of columns.
      *
      * @group typedrel
      * @since 2.0.0
      */
    def dropDuplicatesAndPreserveMetadata(colNames: Seq[String]): DataFrame = {

      val resolver = sparkSession.sessionState.analyzer.resolver
      val allColumns = queryExecution.analyzed.output
      val groupCols = colNames.flatMap { colName =>
        // It is possibly there are more than one columns with the same name,
        // so we call filter instead of find.
        val cols = allColumns.filter(col => resolver(col.name, colName))
        if (cols.isEmpty) {
          throw new AnalysisException(
            s"""Cannot resolve column name "$colName" among (${schema.fieldNames.mkString(", ")})""")
        }
        cols
      }
      val groupColExprIds = groupCols.map(_.exprId)
      val aggCols = logicalPlan.output.map { attr =>
        if (groupColExprIds.contains(attr.exprId)) {
          attr
        } else {
          Alias(new First(attr).toAggregateExpression(), attr.name)(explicitMetadata = Some(attr.metadata))
        }
      }

      Dataset.ofRows(sparkSession, Aggregate(groupCols, aggCols, logicalPlan))

    }
  }

}

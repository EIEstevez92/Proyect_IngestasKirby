package com.datio.kirby.api

package object errors {

  case class KirbyError(code: Int, message: String) {
    override def toString: String = code + " - " + message

    def toFormattedString(params: String*): String = toString.format(params: _*)

    def messageToFormattedString(params: String*): String = message.format(params: _*)
  }

  val INPUT_LABEL = "Input Error: "
  val OUTPUT_LABEL = "Output Error: "
  val REPROCESS_LABEL = "Reprocess Error: "
  val REPARTITION_LABEL = "Repartition Error: "
  val TRANSFORMATION_LABEL = "Transformation Error: "
  val APPLY_FORMAT_LABEL = "Apply Format Error: "


  val INPUT_PATHS_NOT_SET = KirbyError(
    1,
    INPUT_LABEL + "You must configure a path in your input"
  )
  val OUTPUT_REPROCESS_ABORTED = KirbyError(
    2,
    OUTPUT_LABEL + "Job aborted due number of records read and written are different"
  )
  val OUTPUT_REPROCESS_PARTITION_BLOCKED = KirbyError(
    3,
    OUTPUT_LABEL + "Job blocked for possible write error of dataSet partition in path " + "%s " +
      "could leave data inconsistent. You must use 'reprocess=[PARTITION=VALUE]' " +
      "and outputPath of table or use 'force=true' for ignore this check"
  )
  val OUTPUT_OVERWRITE_BLOCKED = KirbyError(
    4,
    OUTPUT_LABEL + "Job blocked for possible %s error of full dataSet in path %s. Use 'force=true' for ignore this check"
  )
  val REPROCESS_DELETE_ABORTED = KirbyError(
    5,
    REPROCESS_LABEL + "Job aborted due impossibility of delete reprocess directory %s"
  )
  val REPROCESS_RENAME_ABORTED = KirbyError(
    6,
    REPROCESS_LABEL + "Job aborted due impossibility of rename directory %s to %s"
  )
  val REPROCESS_EMPTY_LIST = KirbyError(
    7,
    REPROCESS_LABEL + "Mode reprocess is selected but reprocess list is empty"
  )
  val REPROCESS_EMPTY_PARTITION_LIST = KirbyError(
    8,
    REPROCESS_LABEL + "Mode reprocess is selected but partitions list is empty"
  )
  val REPROCESS_MISSED_PARTITION = KirbyError(
    9,
    REPROCESS_LABEL + "All reprocess partitions must be in 'partition' list too with correct order. " +
      "Partitions: %s Missed_reprocess: %s"
  )
  val REPARTITION_NO_PARTITION_PARAMETER = KirbyError(
    10,
    REPARTITION_LABEL + "Coalesce requires 'partitions' parameter"
  )
  val APPLY_FORMAT_INVALID_CASTING = KirbyError(
    11,
    APPLY_FORMAT_LABEL + "ApplyFormatUtil: column %s can not be casted from %s to %s"
  )
  val APPLY_FORMAT_INVALID_CASTING_FIXED_DECIMAL = KirbyError(
    12,
    APPLY_FORMAT_LABEL + "Decimal parser has failed with typeSigned = %s and number = %s"
  )
}

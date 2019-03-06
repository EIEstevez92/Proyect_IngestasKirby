package com.datio.kirby.config.validator

import java.util

import com.datio.kirby.errors.NO_REPORT_AVAILABLE_ERROR
import com.datio.kirby.constants.Constants._
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.core.report.{LogLevel, ProcessingMessage, ProcessingReport}
import com.github.fge.jsonschema.main.JsonSchema

import scala.annotation.tailrec

/** It's used to generate reports for validated json against schema.
  *
  * @param jsonSchemaNode note to be validated
  */
case class JsonValidator(jsonSchemaNode: JsonSchema) {
  protected var validationReport: Option[ProcessingReport] = None

  def generateReport(jsonNode: JsonNode): JsonValidator = {
    validationReport = Some(jsonSchemaNode.validate(jsonNode))
    this
  }

  def report: ProcessingReport = {
    validationReport match {
      case None => throw new ValidatorException(NO_REPORT_AVAILABLE_ERROR)
      case Some(reporting) => reporting
    }
  }

  def messages(minLevel: LogLevel): List[ProcessingMessage] = {
    ReportExtractor.messages(report, minLevel)
  }


  object ReportExtractor extends ReportExtractor

  /** This class work with ProcessingReport and ProcessingMessages
    *
    */
  class ReportExtractor {

    def messages(report: ProcessingReport,
                 minLevel: LogLevel): List[ProcessingMessage] = {

      @tailrec def innerMessages(iter: util.Iterator[ProcessingMessage],
                                 acc: List[ProcessingMessage]): List[ProcessingMessage] = {
        if (iter.hasNext) {
          innerMessages(iter, append(iter.next(), acc))
        } else {
          acc
        }
      }

      def append(message: ProcessingMessage,
                 messages: List[ProcessingMessage]): List[ProcessingMessage] = {
        if (isSeverityEnough(message, minLevel)) {
          message :: messages
        } else {
          messages
        }
      }

      def isSeverityEnough(message: ProcessingMessage, level: LogLevel): Boolean = {
        message.getLogLevel.compareTo(level) >= REPORT_ENOUGH_SEVERITY
      }

      innerMessages(report.iterator(), List())
    }
  }

}
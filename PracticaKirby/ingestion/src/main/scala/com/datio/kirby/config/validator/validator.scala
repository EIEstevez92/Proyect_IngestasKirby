package com.datio.kirby.config

import java.io.File
import java.nio.file.Path

import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.api.errors.KirbyError

import scala.io.Source

package object validator {

  /** Any available input for validator must extends from this.
    *
    */
  trait InputReadable {
    def content: String
  }

  /** Take an String as a classpath resource name to content
    *
    * @param resource path of resource
    */
  case class InputResource(resource: String) extends InputReadable {
    override def content: String = {
      val is = getClass.getClassLoader.getResourceAsStream(resource)
      Source.fromInputStream(is).mkString
    }
  }

  /** Take a File to read the content
    *
    * @param resource path of resource
    */
  case class InputFile(resource: File) extends InputReadable {
    override def content: String = Source.fromFile(resource).mkString
  }

  /** Take an url to read content
    *
    * @param resource path of resource
    */
  case class InputUrl(resource: String) extends InputReadable {
    override def content: String = Source.fromURL(resource).mkString
  }

  /** Take an String as a path name to read content as File
    *
    * @param resource path of resource
    */
  case class InputStringPath(resource: String) extends InputReadable {
    override def content: String = Source.fromFile(resource).mkString
  }

  /** Take a path to read content File
    *
    * @param resource path of resource
    */

  case class InputPath(resource: Path) extends InputReadable {
    override def content: String = Source.fromFile(resource.toFile).mkString
  }

  /** Is an exception wrapper for all errors in Validator
    *
    * @param error Kirby Error
    * @param params optional string params
    */
  class ValidatorException(error:KirbyError, params:String*) extends KirbyException(error, params:_*)

  object InvalidSchemaException {
    def defaultMessage(message: String, cause: Throwable): String =
      if (message != None.orNull) {
        message
      }
      else if (cause != None.orNull) {
        cause.toString
      }
      else {
        None.orNull
      }
  }

  /** Is an exception wrapper for all errors in Validator
    *
    * @param error Kirby error
    * @param params optional params String
    */
  class InvalidSchemaException(error:KirbyError, params:String*) extends KirbyException(error, params:_*)

}
package com.datio.kirby.config

import org.reflections.Reflections

import scala.reflect._
import scala.reflect.runtime.universe._

final class configurable(name: String) extends scala.annotation.StaticAnnotation

trait ConfigHelper {

  /** Return a map of configurable elements ot type `T` in the scope of `pckg`.
    * The classes and names are recovered using reflection
    *
    * @param tag  T typeTag
    * @param pckg package to scan the implementations (is recursive). Default "com.datio.kirby"
    * @return a map with the implementations of `T` with the configurable value as key.
    */
  protected def getImplementations[T](implicit tag: TypeTag[T], pckg: String = "com.datio.kirby"): Map[String, Class[_ <: T]] = {
    import scala.collection.JavaConverters._
    val reflections = new Reflections(pckg)
    val classTag = ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
    val subTypes = reflections.getSubTypesOf[T](classTag.runtimeClass.asInstanceOf[Class[T]]).asScala
    (for {
      impl <- subTypes
      name <- getConfigurableName(impl)
    } yield (name, impl)).toMap
  }

  /** Recover the `com.datio.kirby.config.configurable` annotation value from the class passed by parameter using reflection
    *
    * @param clazz class to recover its configurable annotation value
    * @return the value if the class is configurable
    */
  private def getConfigurableName(clazz: Class[_]): Seq[String] = {
    import scala.reflect.runtime.{universe => u}
    val myAnnotatedClass: ClassSymbol = u.runtimeMirror(Thread.currentThread().getContextClassLoader).classSymbol(clazz)
    val configurable = myAnnotatedClass.annotations.find(a => a.tree.tpe <:< u.typeOf[configurable])
    configurable.flatMap { a =>
      a.tree.children.tail.collect({ case Literal(Constant(id: String)) => id }).headOption
    }.flatMap(a => Some(a.split(",").map(_.trim).toSeq)).getOrElse(Seq())
  }

}




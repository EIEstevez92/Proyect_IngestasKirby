package com.datio.kirby.errors

import com.datio.kirby.api.errors.KirbyError
import com.typesafe.scalalogging.LazyLogging

/**
  * For testing and documentation purposes.
  */
object ObtainErrorList extends LazyLogging {

  val errorPackages = List("com.datio.kirby.api.errors.package$", "com.datio.kirby.errors.package$")

  /**
    * Obtain all the errors in package objects errors and api.errors
    * @return An array of KirbyErrors
    */
  protected[errors] def getAllKirbyErrorsFromPackageObject():Array[KirbyError] =
    getAllKirbyErrorsFromPackageList(errorPackages)

  /**
    * Obtain all the errors in each package of the list
    * @param packages a list with packages names
    * @return An array of KirbyErrors
    */
  protected[errors] def getAllKirbyErrorsFromPackageList(packages:List[String]):Array[KirbyError] = {

    val errors = for{
      pack <- packages
    } yield getKirbyErrorsFromPackageObject(pack)

    errors.fold(Array[KirbyError]())( (a1, a2) => a1 ++ a2)

  }

  /**
    * Obtain all the errors in a package
    * @param pack the qualified name of the package
    * @return An array of KirbyErrors
    */
  protected[errors] def getKirbyErrorsFromPackageObject(pack:String): Array[KirbyError] = {
    import scala.reflect.runtime.universe._
    val runtimeMirror = scala.reflect.runtime.universe.runtimeMirror(this.getClass.getClassLoader)
    val moduleSymbol = runtimeMirror.staticModule(pack)
    val moduleMirror = runtimeMirror.reflectModule(moduleSymbol)
    val moduleInstance = moduleMirror.instance
    val instanceMirror = runtimeMirror.reflect(moduleInstance)

    val errors = instanceMirror.symbol.info.members.collect {
      case m:MethodSymbol if m.isGetter && m.isPublic => m
    }.map(
      m => instanceMirror.reflectField(m).get
    ).filter(
      mb => mb.isInstanceOf[KirbyError]
    ).map(_.asInstanceOf[KirbyError])

    errors.toArray
  }


  /**
    * Print the list of all errors created in packages of Kirby Errors
    * Only for development and documentation purposes.
    *
    * @param args
    */
  def main(args:Array[String]): Unit = {

    val errorList = getAllKirbyErrorsFromPackageObject().map( ke => (ke.code, ke.message.replace("\n", " ")) )

    println("Error code - Error Message")
    print(errorList.sortBy(_._1).map( x => x.productIterator.mkString(" - ")).mkString("\n"))

  }
}

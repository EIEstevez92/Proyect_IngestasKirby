package com.datio.kirby.testUtils

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

trait FileTestUtil {

  def delete(pathS: String) {
    val path = Paths.get(pathS)
    if (Files.isDirectory(path)) {
      val it = Files.list(path).iterator()
      while (it.hasNext) {
        delete(it.next.toString)
      }
    }
    Files.deleteIfExists(path)
  }

  def exists(pathS: String): Boolean = Files.exists(Paths.get(pathS))

  def createDir(path: String): Unit = {
    Files.createDirectory(Paths.get(path))
  }

  def createFile(path: String): Unit = {
    val pw = new PrintWriter(path)
    pw.write("File for testing purpose")
    pw.close()
  }

}

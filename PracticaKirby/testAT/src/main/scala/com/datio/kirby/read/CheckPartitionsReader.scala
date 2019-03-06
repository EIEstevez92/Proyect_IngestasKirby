package com.datio.kirby.read

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

object CheckPartitionsReader {

  def check(path : String, partitionBy: String) : Boolean = {
    val conf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.get(new URI(path), conf)
    val iter: List[URI] = listFiles(fs.listFiles(new Path(path), true))
    checkDirName(partitionBy.split(","), iter)
  }

  private def checkDirName(partitionArray: Array[String], listDir: List[URI]) : Boolean = {
    var res = false

    if (partitionArray.nonEmpty) {
      val collect = listDir.filter(_.getPath.contains(partitionArray.head.trim))
      if (collect.nonEmpty && listDir.nonEmpty) {
        res = checkDirName(partitionArray.tail, listDir)
      }
    } else {
      res = true
    }

    res
  }

  private def listFiles(iter: RemoteIterator[LocatedFileStatus]) = {
    def go(iter: RemoteIterator[LocatedFileStatus], acc: List[URI]): List[URI] = {
      if (iter.hasNext) {
        val uri = iter.next.getPath.toUri
        go(iter, uri :: acc)
      } else {
        acc
      }
    }
    go(iter, List.empty[java.net.URI])
  }

}

package com.datio.kirby.api.util

import java.net.URI

import com.datio.kirby.api.errors.{REPROCESS_DELETE_ABORTED, REPROCESS_RENAME_ABORTED}
import com.datio.kirby.api.exceptions.KirbyException
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.util.{Failure, Success, Try}

trait FileUtils extends LazyLogging {

  /**
    * Given a path return the columns partitions of her subPaths
    *
    * @param path path of table
    * @return List of column partitions names
    */
  protected def getPartitions(path: String): Seq[String] = {
    /**
      * recover partitions from first data file founded
      */
    def getPartitionsRec(pathIterator: RemoteIterator[LocatedFileStatus]): Seq[String] = {
      if (pathIterator.hasNext) {
        val file = pathIterator.next()
        val fileName = file.getPath.toString
        if (file.isFile && (fileName.endsWith("_SUCCESS") || fileName.endsWith("_SUCCESS.crc"))) {
          getPartitionsRec(pathIterator)
        } else {
          extractPartitionFromName(fileName)
        }
      } else {
        Seq[String]()
      }
    }

    /**
      * Method to resolvePath when cannot list files using filesystem (when path is from epsilon)
      */
    def resolvePath(fs: FileSystem, path: String): Seq[String] = fs.globStatus(new org.apache.hadoop.fs.Path(path)).map(_.getPath.getName).toSeq

    def extractPartitionFromName(fileName: String): Seq[String] = {
      fileName.split('/').filter(_.contains('=')).map(_.split('=')(0))
    }

    val fs = FileSystem.get(new URI(path), new Configuration())
    Try(fs.listFiles(new org.apache.hadoop.fs.Path(path), true)) match {
      case Success(pathFilesIterator) => getPartitionsRec(pathFilesIterator)
      case Failure(e) =>
        logger.debug("FileUtils: Recover partition names: error recovering path from filesystem using listFiles", e)
        Try(resolvePath(fs, path)) match {
          case Success(paths) =>
            paths.headOption.map(p => extractPartitionFromName(p)).getOrElse(Seq[String]())
          case Failure(e2) =>
            logger.debug("FileUtils: Recover partition names: error recovering path from filesystem using globStatus", e2)
            Seq[String]()
        }
    }

  }

  /**
    * Delete a directory and subdirectories from HDFS
    *
    * @param path HDFS path
    * @param dir  HDFS directory to delete
    */
  protected def deletePath(path: String, dir: String): Unit = {
    val fs: FileSystem = FileSystem.get(new URI(path), new Configuration())
    val deleted = fs.delete(new org.apache.hadoop.fs.Path(s"$path/$dir"), true)
    if (deleted) {
      logger.info(s"FileUtils: DELETED reprocess directory $path/$dir")
    } else {
      if (fs.exists(new org.apache.hadoop.fs.Path(s"$path/$dir"))) {
        throw new KirbyException(REPROCESS_DELETE_ABORTED, path + "/" + dir)
      } else {
        logger.warn(s"FileUtils: NOT DELETED BECAUSE NOT EXISTS reprocess directory $path/$dir")
      }
    }
  }

  /**
    * Rename a directory in HDFS
    *
    * @param oldPath path to rename
    * @param newpath final path
    */

  protected def renameDirectory(oldPath: String, newpath: String): Unit = {
    val fs: FileSystem = FileSystem.get(new URI(newpath), new Configuration())
    val renamed = fs.rename(new org.apache.hadoop.fs.Path(s"$oldPath"), new org.apache.hadoop.fs.Path(s"$newpath"))
    if (renamed) {
      logger.info(s"FileUtils: RENAMED path from $oldPath to $newpath")
    } else {
      if (fs.exists(new org.apache.hadoop.fs.Path(s"$oldPath"))) {
        throw new KirbyException(REPROCESS_RENAME_ABORTED, oldPath, newpath)
      } else {
        logger.warn(s"FileUtils: NOT DELETED BECAUSE NOT EXISTS directory $oldPath")
      }
    }
  }

}

package com.datio.kirby.read

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object CreatePath {

  def create(path : String) : Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(new URI(path), conf)
    fs.create(new Path(path), true)
    fs.exists(new Path(path))
  }
}

package com.datio.kirby.read

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object CheckPathReader {

  def check(path : String) : Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(new URI(path), conf)
    fs.exists(new Path(path))
  }

}

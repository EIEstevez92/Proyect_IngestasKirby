package com.datio.kirby.read

import scala.sys.process._

object FileInDockerReader {

  def read(pathToRead : String, container : String = "datio_spark"): String = {

    val extractDockerID = ("docker ps -a" #| s"grep $container" #| Seq("awk" , "{print $1}") !!).trim

    val catFile = s"cat $pathToRead"

    val dockerExec = Seq("docker", "exec", extractDockerID, "sh", "-c", catFile)

    dockerExec !!
  }

}

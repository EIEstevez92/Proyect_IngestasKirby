package com.datio.kirby.exec

import scala.language.postfixOps
import scala.sys.process._

object ExecConfig {


  def main(pathConfig: String, envVariables: Map[String, String] = Map()): Int = {

    val SPARK_HOME = "/usr/spark-2.1.0"

    // Activate only for remote debug.
    val REMOTE_DEBUG_ENABLED = false
    val REMOTE_DEBUG_LINE = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"

    val extractDockerID = ("docker ps -a" #| "grep datio_master" #| Seq("awk", "{print $1}") !!).trim

    val sparkSubmit = s"$SPARK_HOME/bin/spark-submit"
    val files = " --files /kirby/caas/caas-cloud.cfg "
    var conf = " --conf spark.sql.shuffle.partitions=3"
    if (REMOTE_DEBUG_ENABLED) {
      conf = s"$conf --driver-java-options $REMOTE_DEBUG_LINE"
    }
    val classToExecute = " --class com.datio.kirby.Launcher"

    val sparkOpts = s"$files $classToExecute $conf --jars /kirby/kirby-api-examples*"
    val kirbyPath = "/kirby/kirby-ingestion*"

    val envCmd = envVariables.filter(_._1.nonEmpty).map(env => s""" export ${env._1}="${env._2}" && """).foldLeft("")(_ + _)
    val debugMode = if (REMOTE_DEBUG_ENABLED) {
      "SPARK_PRINT_LAUNCH_COMMAND=true"
    } else {
      ""
    }
    val runSparkInDocker = s"$envCmd $debugMode $sparkSubmit $sparkOpts $kirbyPath $pathConfig"
    val dockerExec = Seq("docker", "exec", "-i", extractDockerID, "sh", "-c", runSparkInDocker)
    val result = dockerExec.!

    result
  }


}

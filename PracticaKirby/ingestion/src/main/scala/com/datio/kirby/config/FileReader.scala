package com.datio.kirby.config

import java.net.{URI, URL, URLConnection}
import java.security.cert.X509Certificate
import java.util.Base64
import javax.net.ssl._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem}

import scala.io.Source


/**
  * Read a file of input.
  */
trait FileReader {

  /** Get the content of a file from http or HDFS
    *
    * @param path of the file
    * @return content of the file
    */
  def getContentFromPath(path: String): String = {
    if (path.matches("^(http[s]?://).*")) {
      readFromHttp(path)
    } else {
      readFromHdfs(path)
    }
  }

  protected def readFromHttp(path: String): String = {

    //TODO Remove trustful connections when possible
    val connection = trustfulConnection(new URL(path).openConnection)

    val user = sys.env.getOrElse("REPOSITORY_USER", "")
    val password = sys.env.getOrElse("REPOSITORY_PASSWORD", "")

    connection.setRequestProperty(HttpBasicAuth.AUTHORIZATION, HttpBasicAuth.getHeader(user, password))

    Source.fromInputStream(connection.getInputStream).mkString
  }

  protected def readFromHdfs(path: String): String = {
    val fs: FileSystem = FileSystem.get(new URI(path), new Configuration())
    val dis: FSDataInputStream = fs.open(new org.apache.hadoop.fs.Path(path))
    scala.io.Source.fromInputStream(dis).mkString
  }

  private object HttpBasicAuth {
    val BASIC = "Basic"
    val AUTHORIZATION = "Authorization"

    def getHeader(username: String, password: String): String =
      BASIC + " " + new String(Base64.getEncoder.encode(s"$username:$password".getBytes))
  }

  //TODO Remove when possible
  private def trustfulConnection(conn: URLConnection): URLConnection = {
    if (conn.isInstanceOf[HttpsURLConnection]) {
      val https = conn.asInstanceOf[HttpsURLConnection]
      https.setHostnameVerifier(new HostnameVerifier {
        override def verify(s: String, sslSession: SSLSession): Boolean = true
      })
      https.setSSLSocketFactory(trustfulSslContext.getSocketFactory)
    }
    conn
  }

  //TODO Remove when possible
  private def trustfulSslContext: SSLContext = {
    object NoCheckX509TrustManager extends X509TrustManager {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
      override def getAcceptedIssuers: Array[X509Certificate] = Array[X509Certificate]()
    }

    val context = SSLContext.getInstance("TLS")
    context.init(Array[KeyManager](), Array(NoCheckX509TrustManager), None.orNull)
    context
  }

}
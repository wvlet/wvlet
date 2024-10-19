package wvlet.lang.api.server

import wvlet.airframe.http.{Endpoint, HttpMessage, StaticContent}
import wvlet.log.LogSupport

import java.io.File

class StaticContentApi extends LogSupport:
  private val baseDir = sys.props.getOrElse("prog.home", ".")
  info(s"current directory: ${new File(".").getAbsolutePath}")
  info(s"baseDir for static contents: ${baseDir}")

  private val content = StaticContent.fromDirectory(s"${baseDir}/web")

  @Endpoint(path = "/*path")
  def staticContent(path: String): HttpMessage.Response =
    if path.isEmpty then
      content("index.html").withContentType("text/html")
    else
      content(path)

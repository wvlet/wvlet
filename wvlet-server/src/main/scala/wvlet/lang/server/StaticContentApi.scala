package wvlet.lang.server

import wvlet.lang.http.server.static.StaticContent
import wvlet.uni.http.{ContentType, Response}
import wvlet.uni.log.LogSupport

import java.io.File

// Serves static UI assets from ${prog.home}/web. Wired as a fallback handler in WvletServer
// rather than as an @Endpoint-annotated controller: uni's path matcher supports :param
// placeholders but not airframe's /*path splat, so a single catch-all controller endpoint
// isn't expressible. WvletServer composes RPC routing with a serve()-based fallback below.
class StaticContentApi extends LogSupport:
  private val baseDir = sys.props.getOrElse("prog.home", ".")
  trace(s"current directory: ${new File(".").getAbsolutePath}")
  trace(s"baseDir for static contents: ${baseDir}")

  private val content = StaticContent.fromDirectory(s"${baseDir}/web")

  def serve(path: String): Response =
    val key = path.stripPrefix("/")
    if key.isEmpty then
      content("index.html").withContentType(ContentType.TextHtml)
    else
      content(key)

end StaticContentApi

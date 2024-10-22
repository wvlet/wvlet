package wvlet.lang.server

import wvlet.airframe.metrics.ElapsedTime
import wvlet.airframe.ulid.ULID
import wvlet.lang.api.v1.frontend.FrontendApi
import wvlet.lang.api.v1.frontend.FrontendApi.*
import wvlet.lang.api.v1.io.{FileList, FileEntry}
import wvlet.lang.api.v1.query.QueryInfo
import wvlet.lang.compiler.WorkEnv
import wvlet.log.LogSupport

class FrontendApiImpl(queryService: QueryService, workEnv: WorkEnv)
    extends FrontendApi
    with LogSupport:
  private val startTimeNs = System.nanoTime()

  override def status: ServerStatus = ServerStatus(upTime = ElapsedTime.nanosSince(startTimeNs))

  override def submitQuery(request: QueryRequest): QueryResponse =
    debug(s"Received:\n${request}")
    val resp = queryService.enqueue(request)
    resp

  override def getQueryInfo(request: QueryInfoRequest): QueryInfo =
    debug(request)
    queryService.fetchNext(request)

  override def fileList(relativePath: String): FileList =
    val f = new java.io.File(workEnv.path, relativePath)
    val files = f
      .listFiles()
      .collect {
        case d if d.isDirectory && d.getName.startsWith(".") =>
          FileEntry.Directory(f.getName)
        case f if f.getName.endsWith(".wv") =>
          FileEntry.LocalFile(f.getName, f.length())
      }
    FileList(relativePath, files.toList)

  override def readFile(relativePath: String): String =
    // TODO
    ""

  override def saveFile(relativePath: String, content: String): Unit =
    // TODO
    ()

end FrontendApiImpl

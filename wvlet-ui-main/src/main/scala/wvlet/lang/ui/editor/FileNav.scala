package wvlet.lang.ui.editor

import wvlet.airframe.rx
import wvlet.airframe.rx.{Rx, RxVar}
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.svgAttrs.*
import wvlet.lang.api.v1.frontend.FileApi.FileRequest
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.io.FileEntry
import wvlet.lang.ui.component.Icon
import wvlet.lang.ui.editor.FileNav.selectedPath

object FileNav:
  var selectedPath: RxVar[String] = Rx.variable("")

class FileNav(rpcClient: RPCAsyncClient) extends RxElement:
  private def pathElem(elem: RxElement, entry: FileEntry, isRoot: Boolean = false) =
    def pathItem(x: RxElement): RxElement = a(
      href -> "#",
      cls  -> "text-sm font-medium text-gray-500 hover:text-gray-300",
      rx.html
        .when(
          entry.isDirectory,
          onclick -> { e =>
            e.preventDefault()
            rpcClient
              .FileApi
              .listFiles(FileRequest(entry.path))
              .map { lst =>
                info(lst)
              }
          }
        ),
      x
    )
    li(
      cls -> "flex",
      rx.html.when(!isRoot, Icon.slash),
      div(cls -> "flex items-center", pathItem(elem))
    )

  override def render: RxElement = nav(
    cls -> "flex px-2 h-4 text-sm text-gray-400",
    ol(role -> "list", cls -> "flex space-x-4 rounded-md px-1 shadow"),
    selectedPath.map { path =>
      rpcClient
        .FileApi
        .getPath(FileRequest(path))
        .map { pathEntries =>
          info(pathEntries)
          var lst   = pathEntries
          val elems = Seq.newBuilder[RxElement]
          // home directory
          if lst.headOption.exists(_.isDirectory) then
            val home = lst.head
            lst = lst.tail
            elems += pathElem(Icon.home(cls -> "size-4"), home, isRoot = true)
          // path
          lst.map { p =>
            elems += pathElem(p.name, p)
          }
          // If the last path is directory, add "..." to lookup files in the directory
          if pathEntries.lastOption.exists(_.isDirectory) then
            elems += pathElem("...", pathEntries.last)
          elems.result()
        }
    }
  )

end FileNav

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
  private def pathElem(elem: RxElement, parentEntry: FileEntry, isRoot: Boolean = false) =
    def pathItem(x: RxElement): RxElement = a(
      href -> "#",
      cls  -> "text-sm font-medium text-gray-500 hover:text-gray-300",
      // Select the root path
      rx.html
        .when(
          isRoot,
          onclick -> { e =>
            e.preventDefault()
            selectedPath := ""
          }
        ),
      // List files in the directory
      rx.html
        .when(
          !isRoot && parentEntry.isDirectory,
          onclick -> { e =>
            e.preventDefault()
            rpcClient
              .FileApi
              .listFiles(FileRequest(parentEntry.path))
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

  end pathElem

  override def render: RxElement = selectedPath.map { path =>
    nav(
      cls -> "flex px-2 h-4 text-sm text-gray-400",
      ol(role -> "list", cls -> "flex space-x-4 rounded-md px-1 shadow"),
      rpcClient
        .FileApi
        .getPath(FileRequest(path))
        .map { pathEntries =>
          info(pathEntries)

          var parentEntry = FileEntry("", "", true, true, 0, 0)
          val elems       = Seq.newBuilder[RxElement]
          // home directory
          elems +=
            pathElem(Icon.home(cls -> "size-4"), FileEntry("", "", true, true, 0, 0), isRoot = true)

          pathEntries.foreach { p =>
            elems += pathElem(p.name, parentEntry)
            parentEntry = p
          }
          // If the last path is directory, add "..." to lookup files in the directory
          if parentEntry.isDirectory then
            elems += pathElem("...", parentEntry)
          elems.result()
        }
    )
  }

end FileNav

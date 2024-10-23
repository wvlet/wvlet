package wvlet.lang.ui.editor

import org.scalajs.dom
import wvlet.airframe.rx
import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.airframe.rx.html.compat.MouseEvent
import wvlet.airframe.rx.html.svgAttrs.*
import wvlet.airframe.rx.{Rx, RxVar}
import wvlet.airframe.ulid.ULID
import wvlet.lang.api.v1.frontend.FileApi.FileRequest
import wvlet.lang.api.v1.frontend.FrontendRPC.RPCAsyncClient
import wvlet.lang.api.v1.io.FileEntry
import wvlet.lang.ui.component.GlobalState.selectedPath
import wvlet.lang.ui.component.{GlobalState, Icon}

class FileNav(rpcClient: RPCAsyncClient) extends RxElement:

  private val pathElems = Rx.variable(List.empty[PathElem])

  private def hideAll: Unit = pathElems.get.foreach(_.hide)

  case class PathElem(elem: RxElement, parentEntry: FileEntry, isRoot: Boolean = false)
      extends RxElement:
    override def render: RxElement = pathElem(elem, parentEntry, isRoot)
    private val selector           = FileSelectorPopup()

    def hide: Unit = selector.hide

    private def pathElem(elem: RxElement, parentEntry: FileEntry, isRoot: Boolean = false) =
      def pathItem(x: RxElement): RxElement = button(
        // href          -> "#",
        cls           -> "text-sm font-medium text-gray-500 hover:text-gray-300",
        id            -> selector.selectorId,
        aria.expanded -> "true",
        aria.haspopup -> "true",
        // Select the root path
        rx.html
          .when(
            isRoot,
            onclick -> { e =>
              e.preventDefault()
              hideAll
              selectedPath := ""
            }
          ),
        // List files in the directory
        rx.html
          .when(
            !isRoot && parentEntry.isDirectory,
            onclick -> { e =>
              e.preventDefault()
              hideAll
              rpcClient
                .FileApi
                .listFiles(FileRequest(parentEntry.path))
                .map { lst =>
                  selector.updateEntries(lst)
                }
            }
          ),
        x
      )
      li(
        cls -> "flex",
        rx.html.when(!isRoot, Icon.slash),
        div(cls -> "flex items-center", div(cls -> "relative", pathItem(elem), selector))
      )
    end pathElem

  end PathElem

  override def render: RxElement = selectedPath.map { path =>
    nav(
      cls -> "flex px-2 h-4 text-sm text-gray-400",
      ol(role -> "list", cls -> "flex space-x-4 rounded-md px-1 shadow"),
      rpcClient
        .FileApi
        .getPath(FileRequest(path))
        .map { pathEntries =>
          var parentEntry = FileEntry("", "", true, true, 0, 0)
          val elems       = List.newBuilder[PathElem]
          // home directory
          elems +=
            PathElem(Icon.home(cls -> "size-4"), FileEntry("", "", true, true, 0, 0), isRoot = true)

          pathEntries.foreach { p =>
            elems += PathElem(p.name, parentEntry)
            parentEntry = p
          }
          // If the last path is directory, add "..." to lookup files in the directory
          if parentEntry.isDirectory then
            elems += PathElem("...", parentEntry)
          pathElems := elems.result()
          span()
        },
      pathElems
    )
  }

end FileNav

class FileSelectorPopup extends RxElement:
  private val entries = Rx.variable(List.empty[FileEntry])
  private val toShow  = Rx.variable(false)

  val selectorId = s"selector-${ULID.newULID}"
  def updateEntries(lst: List[FileEntry]): Unit =
    entries := lst
    toShow  := true

  def show: Unit = toShow := true
  def hide: Unit = toShow := false

  private val onLostFocus = (e: MouseEvent) => hide

  override def beforeRender: Unit =
    // Close the selector when clicking outside the selector
    dom.document.addEventListener("click", onLostFocus)

  override def beforeUnmount: Unit = dom.document.removeEventListener("click", onLostFocus)

  override def render: RxElement = div(
    cls ->
      "absolute left-0 z-10 mt-0 w-56 origin-top-right divide-y divide-gray-100 rounded-md bg-white shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none overflow-y-scroll max-h-96",
    role             -> "menu",
    aria.orientation -> "vertical",
    aria.labelledby  -> selectorId,
    tabindex         -> "-1",
    toShow
      .filter(_ == true)
      .map { _ =>
        div(
          cls  -> "py-1",
          role -> "none",
          entries.map { lst =>
            lst
              .zipWithIndex
              .map { (e, i) =>
                a(
                  cls ->
                    "group flex items-center px-4 py-1 text-sm text-gray-700 hover:bg-gray-300",
                  role     -> "menuitem",
                  tabindex -> "-1",
                  id       -> s"menu-item-${i}",
                  onclick -> { (event: MouseEvent) =>
                    event.preventDefault()
                    selectedPath := e.path
                  },
                  rx.html.when(e.isDirectory, Icon.folder),
                  span(cls -> "pl-1", e.name)
                )
              }
          }
        )
      }
  )

end FileSelectorPopup

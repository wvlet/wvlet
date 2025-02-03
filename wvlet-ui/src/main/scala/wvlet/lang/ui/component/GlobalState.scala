package wvlet.lang.ui.component

import org.scalajs.dom
import wvlet.airframe.rx.{Rx, RxVar}

import scala.collection.immutable.ListMap

object GlobalState:
  enum Page(val path: String):
    case Editor    extends Page("editor")
    case Converter extends Page("converter")

  case class SelectedPage(page: Page, params: List[(String, String)] = Nil)

  val selectedPage: RxVar[SelectedPage] = Rx.variable(SelectedPage(Page.Editor))

end GlobalState

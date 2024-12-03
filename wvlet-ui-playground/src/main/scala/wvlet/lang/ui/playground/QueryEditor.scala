package wvlet.lang.ui.playground

import wvlet.airframe.rx.{Cancelable, Rx}

class QueryEditor(currentQuery: CurrentQuery, windowSize: WindowSize)
    extends EditorBase(windowSize, "wvlet-editor", "wvlet"):
  override def initialText: String = currentQuery.wvletQuery.get

  private var monitor = Cancelable.empty

  override def onMount: Unit =
    super.onMount
    monitor = Rx
      .intervalMillis(100)
      .map { _ =>
        val query = getText
        if query != currentQuery.wvletQuery.get then
          currentQuery.wvletQuery := query
      }
      .subscribe()

  override def beforeUnmount: Unit =
    super.beforeUnmount
    monitor.cancel

  override protected def action: String => Unit = {
    case "describe-query" =>
      info("Describe query")
    case other =>
      warn(s"Unknown action: ${other}")
  }

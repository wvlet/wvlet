package wvlet.lang.ui.playground

import wvlet.airframe.rx.html.RxElement
import wvlet.airframe.rx.html.all.*
import wvlet.lang.ui.component.MainFrame

import scala.collection.immutable.ListMap

class QueryNavigator(currentQuery: CurrentQuery, queryEditor: QueryEditor) extends RxElement:
  override def render =
    def separator(): RxElement = div(cls -> "border-t border-gray-600 mt-2 mb-2")
    div(
      cls   -> "h-full bg-slate-700 p-3 text-sm text-slate-200 overflow-y-auto",
      style -> s"max-height: calc(100vh - ${MainFrame.navBarHeightPx}px); ",
      h2(cls -> "text-slate-400", "Playground"),
      separator(),
      DemoQuerySet
        .demoQuerySet
        .map { demoQuerySet =>
          div(
            h2(cls -> "text-sm font-bold text-slate-300", demoQuerySet.name),
            ul(
              demoQuerySet
                .queries
                .map { q =>
                  li(
                    cls -> "text-slate-400 hover:text-white px-2",
                    a(
                      href -> "#",
                      q.name,
                      onclick -> { e =>
                        e.preventDefault()
                        currentQuery.setQuery(q)
                        queryEditor.setText(q.query)
                      }
                    )
                  )
                }
            )
          )
        },
      separator()
    )

  end render

end QueryNavigator

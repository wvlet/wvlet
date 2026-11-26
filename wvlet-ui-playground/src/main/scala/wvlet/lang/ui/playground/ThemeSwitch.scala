/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.ui.playground

import org.scalajs.dom
import wvlet.uni.dom.RxElement
import wvlet.uni.dom.all.{*, given}
import wvlet.uni.rx.RxVar
import wvlet.uni.ui.ColorScheme

/**
  * The color theme as three icon buttons (light, dark, follow the system), with the current one
  * pressed. Takes less room in the header than a dropdown.
  */
class ThemeSwitch(theme: RxVar[ColorScheme]) extends RxElement:

  // U+FE0E asks for the text glyph, so iOS does not draw the sun as an emoji
  private def icon(s: ColorScheme): String =
    s match
      case ColorScheme.Light =>
        "☀︎"
      case ColorScheme.Dark =>
        "☾︎"
      case ColorScheme.System =>
        "◐︎"

  private def hint(s: ColorScheme): String =
    s match
      case ColorScheme.System =>
        "System theme"
      case other =>
        s"${other.label} theme"

  override def render: RxElement = div(
    cls       -> "pg-theme",
    role      -> "group",
    ariaLabel -> "Color theme",
    ColorScheme
      .values
      .toSeq
      .map { s =>
        button(
          tpe             -> "button",
          cls             -> "pg-theme__button",
          ariaLabel       -> hint(s),
          titleAttr       -> hint(s),
          aria("pressed") -> theme.map(t => (t == s).toString),
          onclick         -> { (_: dom.MouseEvent) =>
            theme := s
          },
          icon(s)
        )
      }
  )

end ThemeSwitch

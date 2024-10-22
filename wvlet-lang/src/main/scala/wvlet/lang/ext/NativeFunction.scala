package wvlet.lang.ext

import wvlet.airframe.ulid.ULID
import wvlet.lang.api.StatusCode

object NativeFunction:

  def callByName(name: String): Any =
    name match
      case "ulid_string" =>
        ulid_string
      case _ =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unknown function: ${name}")

  def ulid_string: String = ULID.newULIDString

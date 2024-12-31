package wvlet.lang.compiler

object Printer:
  def print(v: Any): String =
    val buf = new StringBuilder()
    def iter(x: Any): Unit =
      x match
        case null =>
        case s: String =>
          buf.append(s)
        case None =>
        case Some(s) =>
          iter(s)
        case a: Seq[?] =>
          a.foreach(iter)
        case x =>
          buf.append(x.toString)

    iter(v)
    buf.result

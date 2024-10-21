package wvlet.lang.runner.cli

object OS:
  def isMacOS: Boolean = sys.props.get("os.name").exists(_.toLowerCase.contains("mac"))

package wvlet.lang.model

import wvlet.uni.reflect.Reflect

trait TreeNodeCompat:
  protected def newInstance(args: Any*): Any =
    val primaryConstructor = this.getClass.getDeclaredConstructors()(0)
    primaryConstructor.newInstance(args*)

  protected def getSingletonObject: Option[Any] =
    val className = this.getClass.getName
    if className.endsWith("$") then
      Reflect.companionOfUnchecked(this.getClass)
    else
      None

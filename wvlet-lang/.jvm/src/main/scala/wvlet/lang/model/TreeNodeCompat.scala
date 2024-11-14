package wvlet.lang.model

import wvlet.airframe.surface.reflect.ReflectTypeUtil

trait TreeNodeCompat:
  protected def newInstance(args: Any*): Any =
    val primaryConstructor = this.getClass.getDeclaredConstructors()(0)
    primaryConstructor.newInstance(args*)

  protected def getSingletonObject: Option[Any] =
    val className = this.getClass.getName
    if className.endsWith("$") then
      ReflectTypeUtil.companionObject(this.getClass)
    else
      None

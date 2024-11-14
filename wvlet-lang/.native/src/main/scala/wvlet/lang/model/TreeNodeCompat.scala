package wvlet.lang.model

import scala.scalanative.reflect.annotation.EnableReflectiveInstantiation
import scala.scalanative.reflect.Reflect

@EnableReflectiveInstantiation
trait TreeNodeCompat:
  protected def newInstance(args: Any*): Any =
    val className   = this.getClass.getName
    val cls         = Reflect.lookupInstantiatableClass(className).get
    val constructor = cls.declaredConstructors(0)
    constructor.newInstance(args*)

  protected def getSingletonObject: Option[Any] =
    val className = this.getClass.getName
    if className.endsWith("$") then
      Reflect.lookupLoadableModuleClass(className).map(_.loadModule())
    else
      None

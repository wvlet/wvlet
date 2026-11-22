package wvlet.lang.skill

import scala.scalajs.js

/**
  * Environment variables for the skill installer. `sys.env` is empty on Scala.js, so read Node's
  * `process.env`.
  */
private[skill] object SkillEnv:
  def get(name: String): Option[String] =
    val v = js.Dynamic.global.process.env.selectDynamic(name)
    if js.isUndefined(v) || v == null then
      None
    else
      Some(v.toString).filter(_.nonEmpty)

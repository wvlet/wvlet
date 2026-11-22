package wvlet.lang.skill

/** Environment variables for the skill installer */
private[skill] object SkillEnv:
  def get(name: String): Option[String] = sys.env.get(name).filter(_.nonEmpty)

package wvlet.lang.model.expr

/**
  * Enhance Seq[Attribute] with some useful methods
  * @param attrs
  */
class AttributeList(val attrs: Seq[Attribute]):
  override def toString: String                           = s"[${attrs.mkString(", ")}]"
  def find(cond: Attribute => Boolean): Option[Attribute] = attrs.find(cond)

object AttributeList:
  def fromSeq(attrs: Seq[Attribute]): AttributeList = new AttributeList(attrs)
  val empty: AttributeList                          = new AttributeList(Seq.empty)

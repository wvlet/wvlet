package wvlet.lang.model.expr

import scala.collection.mutable

object AttributeIndex:
  /**
    * Returns an empty [[AttributeIndex]]
    */
  val empty = AttributeIndex(mutable.LinkedHashSet.empty[AttributeRef])

  /** Constructs a new [[AttributeIndex]] that contains a single [[Attribute]]. */
  def fromAttribute(a: Attribute): AttributeIndex =
    val attrSet = new mutable.LinkedHashSet[AttributeRef]
    attrSet += AttributeRef(a)()
    new AttributeIndex(attrSet)

/**
  * An index structure for holding AttributeRef objects (references to Attribute objects)
  * @param attrSet
  */
class AttributeIndex(attrSet: mutable.LinkedHashSet[AttributeRef])

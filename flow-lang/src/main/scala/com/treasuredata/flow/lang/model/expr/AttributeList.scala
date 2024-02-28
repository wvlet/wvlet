package com.treasuredata.flow.lang.model.expr

/**
  * Enhance Seq[Attribute] with some useful methods
  * @param attrs
  */
class AttributeList(val attrs: Seq[Attribute]) {}

object AttributeList:
  def fromSeq(attrs: Seq[Attribute]): AttributeList = new AttributeList(attrs)

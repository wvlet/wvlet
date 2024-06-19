package com.treasuredata.flow.lang.model.expr

//case class TableIdentifier(catalog: Option[String], database: Option[String], table: String)

class Qualifier(val parts: Seq[String]):
  def prefix: String = parts.mkString(".")
  def qualifiedName(name: String): String =
    if parts.isEmpty then
      name
    else
      s"${prefix}.$name"

  def isEmpty: Boolean               = parts.isEmpty
  def +(other: Qualifier): Qualifier = Qualifier(parts ++ other.parts)

object Qualifier:
  val empty = Qualifier(Seq.empty)

  def parse(s: String): Qualifier =
    s.split("\\.").toSeq match
      case s if s.isEmpty =>
        empty
      case parts =>
        Qualifier(parts)

package com.treasuredata.flow.lang.compiler

/**
  * Name represents a single leaf string value of term names (e.g., function name, variable, package
  * name) or type names (int, string, or user-defined types).
  *
  * Name is used for lookup entries in the symbol table (Scope) and Name needs to be created with
  * [[Name.termName]] or [[Name.typeName]] to ensure uniqueness of the name.
  *
  * The meaning of Name can be different depending on the current Scope. To resolve the meaning of
  * Name, its corresponding Symbol needs to be looked up in the current Scope, and resolve its
  * SymbolInfo.
  *
  *   - [[Name]] (string)
  *   - Search the current [[Scope]] (= SymbolTable) for the corresponding [[Symbol]]
  *   - From the [[Symbol]], check its [[SymbolInfo]] ([[DataType]], TypeDef, etc.), which will be
  *     refined as compiler phases proceed.
  */
sealed abstract class Name(val name: String) derives CanEqual:
  override def toString: String = name
  def isTermName: Boolean
  def isTypeName: Boolean
  def isEmpty: Boolean = this eq Name.NoName

  override def hashCode: Int              = System.identityHashCode(this)
  override def equals(that: Any): Boolean = this eq that.asInstanceOf[AnyRef]
end Name

case class TermName private[compiler] (override val name: String) extends Name(name):
  private lazy val _typeName = TypeName(name)
  def toTypeName: TypeName   = _typeName

  override def isTermName: Boolean = true
  override def isTypeName: Boolean = false

case class TypeName private[compiler] (override val name: String) extends Name(name):
  override def isTermName: Boolean = false
  override def isTypeName: Boolean = true
  def toTermName: TermName         = Name.termName(name)

object Name:
  // Manages a set of unique TermName objects
  private val nameTable = collection.mutable.Map.empty[String, TermName]

  val NoName     = termName("<NoName>")
  val NoTypeName = typeName("<NoType>")

  def termName(s: String): TermName = nameTable.getOrElseUpdate(s, TermName(s))
  def typeName(s: String): TypeName =
    // derive type name from term name
    termName(s).toTypeName

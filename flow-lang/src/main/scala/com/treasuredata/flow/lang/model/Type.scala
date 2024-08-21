package com.treasuredata.flow.lang.model

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.{Name, TypeName}
import com.treasuredata.flow.lang.model.DataType.{NamedType, TypeParameter}
import com.treasuredata.flow.lang.model.expr.Expression
import com.treasuredata.flow.lang.model.plan.Import

import scala.quoted.Expr

abstract class Type:
  def typeDescription: String
  def isFunctionType: Boolean = false
  def isResolved: Boolean
  def isBound: Boolean = true
  def bind(typeArgMap: Map[TypeName, DataType]): DataType =
    this match
      case d: DataType =>
        d
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Cannot bind type ${other}")

  def typeParams: Seq[DataType] = Nil

object Type:
  val UnknownType: Type =
    new Type:
      override def typeDescription: String = "<Unknown>"
      override def isResolved: Boolean     = false

  case class ImportType(i: Import) extends Type:
    override def typeDescription: String = s"import ${i.importRef}"
    override def isResolved: Boolean     = true

  case class PackageType(name: Name) extends Type:
    override def typeDescription: String = s"package ${name}"
    override def isResolved: Boolean     = true

  abstract class LazyType extends (Symbol => LazyType):
    def apply(symbol: Symbol): LazyType

  case class FunctionType(
      name: Name,
      args: Seq[NamedType],
      returnType: DataType,
      contextNames: List[Name]
  ) extends Type:
    override def toString: String        = typeDescription
    override def typeDescription: String = s"${name}(${args.mkString(", ")}): ${returnType}"
    override def isFunctionType: Boolean = true
    override def isResolved: Boolean     = args.forall(_.isResolved) && returnType.isResolved

end Type

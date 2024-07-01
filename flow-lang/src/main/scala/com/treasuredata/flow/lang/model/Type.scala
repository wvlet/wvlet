package com.treasuredata.flow.lang.model

import com.treasuredata.flow.lang.compiler.Name
import com.treasuredata.flow.lang.model.expr.Expression
import com.treasuredata.flow.lang.model.plan.Import

import scala.quoted.Expr

class Type()

object Type:
  val UnknownType: Type = Type()

case class ImportType(i: Import) extends Type

case class PackageType(name: Name) extends Type

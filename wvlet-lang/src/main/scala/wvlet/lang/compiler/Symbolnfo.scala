/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler

import wvlet.lang.model.{DataType, Type}
import Type.{FunctionType, PackageType}
import wvlet.lang.compiler.Symbol.NoSymbol
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.plan.{DefContext, EmptyRelation, LogicalPlan}
import wvlet.log.LogSupport

enum SymbolType:
  case Undefined
  case Error
  case Package
  case Import
  case ModelDef
  case TypeDef
  case MethodDef
  case ValDef
  case Relation
  case Query
  case Expression

/**
  * SymbolInfo is the result of resolving a name (Symbol) during the compilation phase.
  *
  * @param symbol
  * @param owner
  * @param name
  * @param dataType
  */
class SymbolInfo(
    val symbolType: SymbolType,
    val owner: Symbol,
    val symbol: Symbol,
    val name: Name,
    private var _tpe: Type
):
  override def toString(): String =
    if owner.isDefined then
      s"[${symbolType}] ${owner}.${name}: ${tpe}"
    else
      s"[${symbolType}] ${name}: ${tpe}"

  private var _declScope: Scope | Null = null
  def declScope: Scope                 = _declScope
  def declScope_=(s: Scope): Unit      = _declScope = s

  def tpe: Type = _tpe
  def withType(t: Type): this.type =
    _tpe = t
    this

  def dataType =
    tpe match
      case t: DataType =>
        t
      case _ =>
        DataType.UnknownType

  def withDataType(d: DataType): this.type = withType(d)

  def findMember(name: Name): Symbol = NoSymbol
  def members: List[Symbol]          = Nil
  def typeParams: Seq[DataType]      = Nil

end SymbolInfo

class PackageSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val tpe: PackageType,
    packageScope: Scope
) extends SymbolInfo(SymbolType.Package, owner, symbol, name, tpe):
  this.declScope = packageScope

  override def toString: String =
    if owner.isNoSymbol then
      s"${name}"
    else
      s"${owner}.${name}"

object NoSymbolInfo
    extends SymbolInfo(
      SymbolType.Undefined,
      Symbol.NoSymbol,
      Symbol.NoSymbol,
      Name.NoName,
      Type.UnknownType
    ):
  override def toString: String = "NoSymbol"

class TypeSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    tpe: DataType,
    override val typeParams: Seq[DataType],
    typeScope: Scope
) extends SymbolInfo(SymbolType.TypeDef, symbol, owner, name, tpe):
  this.declScope = typeScope

  override def toString: String               = s"${owner}.${name}: ${dataType}"
  override def findMember(name: Name): Symbol = typeScope.lookupSymbol(name).getOrElse(NoSymbol)
  override def members: List[Symbol]          = typeScope.getLocalSymbols

case class MethodSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    ft: FunctionType,
    body: Option[Expression],
    defContexts: List[DefContext]
) extends SymbolInfo(SymbolType.MethodDef, owner, symbol, name, ft)
    with LogSupport:
  override def toString: String = s"${owner}.${name}: ${ft}"

  def bind(typeArgMap: Map[TypeName, DataType]): MethodSymbolInfo =
    val newFT = ft.bind(typeArgMap)
    this.copy(ft = newFT)

case class ModelSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val tpe: DataType,
    compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.ModelDef, symbol, owner, name, tpe):
  override def toString: String = s"model ${owner}.${name}: ${dataType}"

case class ValSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val tpe: DataType,
    expr: Expression
) extends SymbolInfo(SymbolType.ValDef, Symbol.NoSymbol, symbol, name, tpe):
  override def toString: String = s"bounded ${name}: ${dataType} = ${expr}"

case class MultipleSymbolInfo(s1: SymbolInfo, s2: SymbolInfo)
    extends SymbolInfo(s1.symbolType, s1.owner, s1.symbol, s1.name, s1.tpe)

case class RelationAliasSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.Relation, owner, symbol, name, DataType.UnknownType)

case class SavedRelationSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name
) extends SymbolInfo(SymbolType.Relation, owner, symbol, name, DataType.UnknownType)

case class QuerySymbol(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name
) extends SymbolInfo(SymbolType.Query, owner, symbol, name, DataType.UnknownType)

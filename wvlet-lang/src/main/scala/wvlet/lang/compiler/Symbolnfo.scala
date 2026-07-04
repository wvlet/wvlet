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

import wvlet.lang.model.DataType
import wvlet.lang.model.Type
import Type.FunctionType
import Type.PackageType
import wvlet.lang.compiler.Symbol.NoSymbol
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.plan.DefArg
import wvlet.lang.model.plan.DefContext
import wvlet.lang.model.plan.EmptyRelation
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.Relation
import wvlet.uni.log.LogSupport

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
  case PartialQueryDef
  case FlowDef

/**
  * SymbolInfo is the result of resolving a name (Symbol) during the compilation phase. SymbolInfo
  * is immutable: replacing the resolution result of a symbol (e.g., re-defining a model in REPL)
  * assigns a new SymbolInfo to the Symbol instead of mutating the existing one
  *
  * @param symbol
  * @param owner
  * @param name
  * @param tpe
  */
class SymbolInfo(
    val symbolType: SymbolType,
    val owner: Symbol,
    val symbol: Symbol,
    val name: Name,
    val tpe: Type
):
  override def toString(): String =
    if owner.isDefined then
      s"[${symbolType}] ${owner}.${name}: ${tpe}"
    else
      s"[${symbolType}] ${name}: ${tpe}"

  /**
    * The scope of the symbols declared inside this symbol (e.g., models in a package, methods in a
    * type definition)
    */
  def declScope: Scope = Scope.NoScope

  /**
    * The compilation unit where this symbol is defined. Symbols that are not tied to a single
    * source definition (e.g., packages, builtin symbols) return CompilationUnit.empty
    */
  def compilationUnit: CompilationUnit = CompilationUnit.empty

  def dataType =
    tpe match
      case t: DataType =>
        t
      case _ =>
        DataType.UnknownType

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
  override def declScope: Scope = packageScope

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
      Type.NoType
    ):
  override def toString: String = "NoSymbol"

class TypeSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    tpe: DataType,
    override val typeParams: Seq[DataType],
    typeScope: Scope,
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.TypeDef, owner, symbol, name, tpe):
  override def declScope: Scope = typeScope

  override def toString: String               = s"${owner}.${name}: ${dataType}"
  override def findMember(name: Name): Symbol = typeScope.lookupSymbol(name).getOrElse(NoSymbol)
  override def members: List[Symbol]          = typeScope.getLocalSymbols

case class MethodSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    ft: FunctionType,
    body: Option[Expression],
    defContexts: List[DefContext],
    override val compilationUnit: CompilationUnit
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
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.ModelDef, owner, symbol, name, tpe):
  override def toString: String = s"model ${owner}.${name}: ${dataType}"

/**
  * Symbol info for partial query definitions. Partial queries are reusable query fragments (where,
  * select, etc.) that can be applied via pipe.
  *
  * @param owner
  *   The owning symbol (typically package)
  * @param symbol
  *   The symbol for this partial query
  * @param name
  *   The name of the partial query
  * @param params
  *   Parameters for the partial query
  * @param body
  *   The relation body (operators like Filter, Project, etc. with EmptyRelation as input)
  * @param compilationUnit
  *   The compilation unit where this is defined
  */
case class PartialQuerySymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    params: List[DefArg],
    body: Relation,
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.PartialQueryDef, owner, symbol, name, DataType.UnknownType):
  override def toString: String = s"partial ${owner}.${name}(${params.map(_.name).mkString(", ")})"

/**
  * Symbol info for flow definitions. Flows are orchestration containers of stages that are
  * triggered explicitly (run flow), by schedules, or by dependencies.
  *
  * @param owner
  *   The owning symbol (typically package)
  * @param symbol
  *   The symbol for this flow
  * @param name
  *   The name of the flow
  * @param compilationUnit
  *   The compilation unit where this flow is defined
  */
case class FlowSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.FlowDef, owner, symbol, name, DataType.UnknownType):
  override def toString: String = s"flow ${owner}.${name}"

case class ValSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val tpe: DataType,
    expr: Expression,
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.ValDef, owner, symbol, name, tpe):
  override def toString: String =
    tpe match
      case schemaType: DataType.SchemaType =>
        s"table ${name}(${schemaType
            .columnTypes
            .map(c => s"${c.name}: ${c.dataType}")
            .mkString(", ")})"
      case _ =>
        s"bounded ${name}: ${dataType} = ${expr}"

case class MultipleSymbolInfo(s1: SymbolInfo, s2: SymbolInfo)
    extends SymbolInfo(s1.symbolType, s1.owner, s1.symbol, s1.name, s1.tpe):
  override def compilationUnit: CompilationUnit = s1.compilationUnit

case class RelationAliasSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.Relation, owner, symbol, name, DataType.UnknownType)

case class SavedRelationSymbolInfo(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.Relation, owner, symbol, name, DataType.UnknownType)

case class QuerySymbol(
    override val owner: Symbol,
    override val symbol: Symbol,
    override val name: Name,
    override val compilationUnit: CompilationUnit
) extends SymbolInfo(SymbolType.Query, owner, symbol, name, DataType.UnknownType)

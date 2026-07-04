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

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

object GlobalSymbolIndex:
  /**
    * A top-level definition of a name: the compilation unit that defines it and its symbol
    * @param unit
    * @param symbol
    */
  case class Entry(unit: CompilationUnit, symbol: Symbol):
    def fileName: String = unit.sourceFile.fileName

/**
  * A package-level index of the top-level symbols of all compilation units, following the
  * package-denotation design of the Scala 3 compiler (issue #1811).
  *
  * Non-preset units are grouped by their package name (an empty string is the shared default
  * package), while preset (standard library) units are indexed separately because they are visible
  * from every package. Looking up a name consults only the buckets of the packages visible from the
  * current unit (preset, default package, the unit's own package, and imported packages) instead of
  * scanning the known symbols of every compilation unit.
  */
class GlobalSymbolIndex:
  import GlobalSymbolIndex.*

  /**
    * Index of the top-level names defined in one package. The entries of each name are ordered by
    * the defining source file name (newest definition first within the same unit) so that a name
    * defined in multiple files resolves deterministically, regardless of the compilation order
    */
  private class PackageIndex:
    val members = new ConcurrentHashMap[Name, List[Entry]]()

    def add(entry: Entry): Unit = members.compute(
      entry.symbol.name,
      (_, current) =>
        val entries = Option(current).getOrElse(Nil)
        // Insert ordered by file name. A new entry of an already-indexed unit is placed before
        // the unit's existing entries, so the most recent definition in a unit wins, matching
        // the newest-first order of CompilationUnit.knownSymbols
        val (before, after) = entries.span(_.fileName < entry.fileName)
        before ::: entry :: after
    )

    def get(name: Name): List[Entry] = Option(members.get(name)).getOrElse(Nil)

    def removeUnit(unit: CompilationUnit): Unit = members
      .keySet()
      .asScala
      .foreach { name =>
        members.computeIfPresent(
          name,
          (_, entries) =>
            entries.filterNot(_.unit eq unit) match
              case Nil =>
                null
              case remaining =>
                remaining
        )
      }

  end PackageIndex

  // Top-level symbols of non-preset units, grouped by package name
  private val packageIndexes = new ConcurrentHashMap[String, PackageIndex]()
  // Top-level symbols of preset (standard library) units, visible from every package
  private val presetIndex = PackageIndex()

  private def indexOf(unit: CompilationUnit): PackageIndex =
    if unit.isPreset then
      presetIndex
    else
      packageIndexes.computeIfAbsent(unit.packageName, _ => PackageIndex())

  /**
    * Register a top-level symbol of the given compilation unit. Symbols without a name (e.g.,
    * anonymous command statements) are kept only in the unit's knownSymbols since they can never be
    * found by name
    */
  def add(unit: CompilationUnit, symbol: Symbol): Unit =
    if !symbol.name.isEmpty then
      indexOf(unit).add(Entry(unit, symbol))

  /**
    * Register all known symbols of a unit that was already labeled in a previous compilation run
    * (e.g., preset standard library units shared between compilers). The symbols are indexed in
    * their original entry order
    */
  def addAll(unit: CompilationUnit): Unit =
    if unit.knownSymbols.nonEmpty then
      val index = indexOf(unit)
      // knownSymbols is newest-first, so replay the entries in chronological order
      unit
        .knownSymbols
        .reverseIterator
        .foreach { symbol =>
          if !symbol.name.isEmpty then
            index.add(Entry(unit, symbol))
        }

  /**
    * Remove all symbols of the given unit from the index, e.g., before the unit is reloaded for
    * recompilation
    */
  def remove(unit: CompilationUnit): Unit =
    presetIndex.removeUnit(unit)
    packageIndexes.values().asScala.foreach(_.removeUnit(unit))

  /**
    * All definitions of the given name that are visible from a unit in `currentPackage` with the
    * given import references, ordered by the defining source file name. Preset units and units in
    * the default package are always visible; other units are visible only from the same package or
    * through an import of the package or one of its members (#93)
    */
  def visibleEntries(name: Name, currentPackage: String, importRefs: List[String]): List[Entry] =
    val visiblePackages = List.newBuilder[String]
    visiblePackages += ""
    if currentPackage.nonEmpty then
      visiblePackages += currentPackage
    // An import of a package or one of its members (e.g., import a.b.c) makes the package and
    // all of its enclosing packages (a.b.c, a.b, a) visible
    importRefs.foreach { ref =>
      visiblePackages += ref
      var dot = ref.lastIndexOf('.')
      while dot > 0 do
        visiblePackages += ref.substring(0, dot)
        dot = ref.lastIndexOf('.', dot - 1)
    }
    val entries = List.newBuilder[Entry]
    entries ++= presetIndex.get(name)
    visiblePackages
      .result()
      .distinct
      .foreach { pkg =>
        Option(packageIndexes.get(pkg)).foreach(index => entries ++= index.get(name))
      }
    // Each bucket is already ordered, so a stable sort restores the global file-name order
    entries.result().sortBy(_.fileName)

  /**
    * The first definition of the given name across all packages in file-name order, ignoring
    * package visibility. Used for names explicitly made visible by an import of the name itself
    */
  def findFirstAnywhere(name: Name): Option[Entry] =
    val entries = List.newBuilder[Entry]
    entries ++= presetIndex.get(name)
    packageIndexes.values().asScala.foreach(index => entries ++= index.get(name))
    entries.result().sortBy(_.fileName).headOption

end GlobalSymbolIndex

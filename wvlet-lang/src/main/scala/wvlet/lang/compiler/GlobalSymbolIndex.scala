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
    // Cached because the file name is the sort key of every lookup
    val fileName: String = unit.fileName

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
    * Index of the top-level names defined in one package. The entries of each name are kept
    * newest-first (matching the order of CompilationUnit.knownSymbols); [[sortedByFileName]]
    * restores the deterministic file-name resolution order on lookup
    */
  private class PackageIndex:
    val members = ConcurrentHashMap[Name, List[Entry]]()

    def add(entry: Entry): Unit = members.compute(
      entry.symbol.name,
      (_, current) => entry :: Option(current).getOrElse(Nil)
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

  /**
    * Registration state of one unit: the bucket its symbols were added to (so removal targets the
    * right bucket even if the unit's package declaration changes), and the knownSymbols list that
    * was current at registration time (so [[refresh]] can cheaply detect an unchanged unit by
    * reference, as knownSymbols is an immutable list replaced on every change)
    */
  private case class Registration(bucket: PackageIndex, knownSymbols: List[Symbol])

  // Top-level symbols of non-preset units, grouped by package name
  private val packageIndexes = ConcurrentHashMap[String, PackageIndex]()
  // Top-level symbols of preset (standard library) units, visible from every package
  private val presetIndex = PackageIndex()
  // The registration state of each indexed unit
  private val registrations = ConcurrentHashMap[CompilationUnit, Registration]()

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
      val index = indexOf(unit)
      registrations.put(unit, Registration(index, unit.knownSymbols))
      index.add(Entry(unit, symbol))

  /**
    * Register all known symbols of a unit that was already labeled in a previous compilation run
    * (e.g., preset standard library units shared between compilers). The symbols are indexed in
    * their original entry order
    */
  def addAll(unit: CompilationUnit): Unit =
    // knownSymbols is newest-first, so replay the entries in chronological order
    unit.knownSymbols.reverseIterator.foreach(add(unit, _))

  /**
    * Remove all symbols of the given unit from the index, e.g., when the unit is re-labeled after a
    * reload
    */
  def remove(unit: CompilationUnit): Unit = Option(registrations.remove(unit)).foreach(
    _.bucket.removeUnit(unit)
  )

  /**
    * Re-index a unit whose known symbols changed since it was registered, e.g., a preset unit
    * shared between compilers that was labeled after this index took its registration snapshot. A
    * no-op when the unit's known symbols are unchanged
    */
  def refresh(unit: CompilationUnit): Unit =
    val current = registrations.get(unit)
    if (current == null && unit.knownSymbols.nonEmpty) ||
      (current != null && (current.knownSymbols ne unit.knownSymbols))
    then
      remove(unit)
      addAll(unit)

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
    val visibleIndexes = visiblePackages
      .result()
      .distinct
      .flatMap(pkg => Option(packageIndexes.get(pkg)))
    sortedByFileName(entriesIn(name, visibleIndexes))

  /**
    * The first definition of the given name across all packages in file-name order, ignoring
    * package visibility. Used for names explicitly made visible by an import of the name itself
    */
  def findFirstAnywhere(name: Name): Option[Entry] = entriesIn(
    name,
    packageIndexes.values().asScala
  ).minByOption(_.fileName)

  /**
    * All entries of the given name in the preset index and the given package indexes, in bucket
    * order (newest definition first within a unit)
    */
  private def entriesIn(name: Name, indexes: IterableOnce[PackageIndex]): List[Entry] =
    val entries = List.newBuilder[Entry]
    entries ++= presetIndex.get(name)
    indexes.iterator.foreach(index => entries ++= index.get(name))
    entries.result()

  /**
    * Order the merged bucket entries by the defining source file name. The sort is stable, so
    * entries of the same unit stay newest-first
    */
  private def sortedByFileName(entries: List[Entry]): List[Entry] =
    if entries.sizeIs <= 1 then
      entries
    else
      entries.sortBy(_.fileName)

end GlobalSymbolIndex

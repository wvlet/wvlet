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
package wvlet.lang.engine.catalog

import wvlet.lang.compiler.SourceIO
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.*
import wvlet.uni.log.LogSupport

import scala.util.control.NonFatal

/** A foreign key: `columns` of a table reference `referencedColumns` of table `references` */
case class ForeignKey(columns: Seq[String], references: String, referencedColumns: Seq[String])

/**
  * The keys a sidecar file declares for a Parquet file or folder: unique column sets (the primary
  * key included) and foreign keys
  */
case class TableKeys(uniqueKeys: Seq[Seq[String]] = Nil, foreignKeys: Seq[ForeignKey] = Nil):
  def isEmpty: Boolean = uniqueKeys.isEmpty && foreignKeys.isEmpty

  /** These keys without the ones naming a column that `columns` doesn't have */
  def over(columns: Seq[String]): TableKeys =
    val names                             = columns.map(_.toLowerCase).toSet
    def known(cols: Seq[String]): Boolean =
      cols.nonEmpty && cols.forall(c => names.contains(c.toLowerCase))
    TableKeys(uniqueKeys.filter(known), foreignKeys.filter(f => known(f.columns)))

/**
  * Sidecar key files of Parquet data (`<name>.keys.json` next to `<name>.parquet`, or `_keys.json`
  * in a folder), read by [[ParquetSource]]:
  * {{{
  * {
  *   "primaryKey": ["l_orderkey", "l_linenumber"],
  *   "uniqueKeys": [["l_comment"]],
  *   "foreignKeys": [
  *     {"columns": ["l_partkey", "l_suppkey"], "references": "partsupp",
  *      "referencedColumns": ["ps_partkey", "ps_suppkey"]}
  *   ]
  * }
  * }}}
  * A folder's `_keys.json` may instead hold `{"tables": {"lineitem": {...}, ...}}`, the keys of the
  * Parquet files in that folder by table name. A file that can't be read or parsed declares no keys
  * (with a warning), as does a key naming a column the table doesn't have
  */
object TableKeys extends LogSupport:
  /** The sidecar file name of a folder */
  val FolderFile = "_keys.json"

  /** The suffix of a file's sidecar, replacing `.parquet` */
  val FileSuffix = ".keys.json"

  /** The keys declared for the Parquet file or folder at `path`, or empty */
  def forPath(path: String): TableKeys =
    if path.contains("://") && !path.startsWith("file://") then
      // Remote data: one more request per table reference isn't worth it until it is cached
      TableKeys()
    else
      val p    = path.stripSuffix("/")
      val name = p.split("[/\\\\]").last
      val dir  = p.dropRight(name.length).stripSuffix("/").stripSuffix("\\")
      if p.toLowerCase.endsWith(".parquet") && !p.exists(c => c == '*' || c == '{') then
        val stem = name.dropRight(".parquet".length)
        val own  = Seq(s"${dir}/${stem}${FileSuffix}", s"${p}${FileSuffix}").find(exists)
        own match
          case Some(f) =>
            read(f).map(parse(_, f)).getOrElse(TableKeys())
          case None =>
            // The folder's keys of this table
            val folder = s"${dir}/${FolderFile}"
            if exists(folder) then
              read(folder)
                .map(tablesOf(_, folder).getOrElse(stem.toLowerCase, TableKeys()))
                .getOrElse(TableKeys())
            else
              TableKeys()
      else if SourceIO.isDirectory(p) then
        val folder = s"${p}/${FolderFile}"
        if exists(folder) then
          read(folder).map(parse(_, folder)).getOrElse(TableKeys())
        else
          TableKeys()
      else
        TableKeys()

  private def exists(path: String): Boolean =
    try
      SourceIO.existsFile(path)
    catch
      case NonFatal(_) =>
        false

  private def read(path: String): Option[JSONValue] =
    try
      Some(JSON.parse(SourceIO.readAsString(path)))
    catch
      case NonFatal(e) =>
        warn(s"Ignoring key declarations in ${path}: ${e.getMessage}")
        None

  /** The keys of each table in a folder file's `tables` object, by lower-case name */
  private def tablesOf(json: JSONValue, file: String): Map[String, TableKeys] =
    json match
      case o: JSONObject =>
        o.get("tables") match
          case Some(t: JSONObject) =>
            t.v.map((name, spec) => name.toLowerCase -> parse(spec, s"${file} (${name})")).toMap
          case _ =>
            Map.empty
      case _ =>
        Map.empty

  /** The keys of one table spec; malformed parts are ignored with a warning */
  def parse(json: JSONValue, file: String = "keys"): TableKeys =
    def strings(v: JSONValue): Option[Seq[String]] =
      v match
        case JSONArray(vs) if vs.forall(_.isInstanceOf[JSONString]) =>
          Some(
            vs.collect { case JSONString(s) =>
              s
            }
          )
        case _ =>
          None
    def invalid(what: String): Unit = warn(
      s"Ignoring ${what} in ${file}: expected a list of columns"
    )
    json match
      case o: JSONObject =>
        val primary = o
          .get("primaryKey")
          .toSeq
          .flatMap { v =>
            val k = strings(v)
            if k.isEmpty then
              invalid("primaryKey")
            k
          }
        val unique = o
          .get("uniqueKeys")
          .toSeq
          .flatMap {
            case JSONArray(ks) =>
              ks.flatMap { k =>
                val cols = strings(k)
                if cols.isEmpty then
                  invalid("a unique key")
                cols
              }
            case _ =>
              invalid("uniqueKeys")
              Nil
          }
        val foreign = o
          .get("foreignKeys")
          .toSeq
          .flatMap {
            case JSONArray(fks) =>
              fks.flatMap {
                case f: JSONObject =>
                  val fk =
                    for
                      cols <- f.get("columns").flatMap(strings)
                      ref  <- f
                        .get("references")
                        .collect { case JSONString(t) =>
                          t
                        }
                      refCols = f.get("referencedColumns").flatMap(strings).getOrElse(Nil)
                    yield ForeignKey(cols, ref, refCols)
                  if fk.isEmpty then
                    warn(s"Ignoring a foreign key in ${file}: it needs columns and references")
                  fk
                case _ =>
                  warn(s"Ignoring a foreign key in ${file}: expected an object")
                  None
              }
            case _ =>
              warn(s"Ignoring foreignKeys in ${file}: expected a list")
              Nil
          }
        TableKeys((primary ++ unique).filter(_.nonEmpty).distinct, foreign)
      case _ =>
        warn(s"Ignoring key declarations in ${file}: expected a JSON object")
        TableKeys()
    end match

  end parse

end TableKeys

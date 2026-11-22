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
package wvlet.lang.engine.vector

/**
  * A column of an operator's output. `qualifiers` are the relation names the column can be
  * referenced through (`t.c`): the table name of a scan, an alias, or the enclosing names of a join
  * input
  */
case class Field(name: String, ptype: PType, qualifiers: List[String] = Nil):
  def withQualifier(q: String): Field = copy(qualifiers = List(q))
  def noQualifier: Field              = copy(qualifiers = Nil)
  def withName(n: String): Field      = copy(name = n)

case class Schema(fields: IndexedSeq[Field]):
  def size: Int                            = fields.size
  def names: IndexedSeq[String]            = fields.map(_.name)
  def types: IndexedSeq[PType]             = fields.map(_.ptype)
  def apply(i: Int): Field                 = fields(i)
  def ++(other: Schema): Schema            = Schema(fields ++ other.fields)
  def withQualifier(q: String): Schema     = Schema(fields.map(_.withQualifier(q)))
  def mapFields(f: Field => Field): Schema = Schema(fields.map(f))

object Schema:
  val empty: Schema = Schema(IndexedSeq.empty)

/** A set of equally sized column vectors */
final class Batch(val columns: Array[Vec], val size: Int):
  def numColumns: Int         = columns.length
  def column(i: Int): Vec     = columns(i)
  def isEmpty: Boolean        = size == 0
  def row(i: Int): Array[Any] = columns.map(_.get(i))

  def filter(mask: Array[Boolean]): Batch =
    var count = 0
    var i     = 0
    while i < size do
      if mask(i) then
        count += 1
      i += 1
    if count == size then
      this
    else
      Batch(columns.map(_.filter(mask, count)), count)

  def gather(idx: Array[Int], n: Int): Batch = Batch(columns.map(_.gather(idx, n)), n)

  def slice(from: Int, until: Int): Batch =
    if from == 0 && until >= size then
      this
    else
      Batch(columns.map(_.slice(from, until)), math.max(0, math.min(until, size) - from))

  def select(indexes: Array[Int]): Batch = Batch(indexes.map(columns), size)

  override def toString: String = s"Batch(${size} rows, ${numColumns} columns)"

object Batch:
  val DefaultSize = 4096

  def empty(schema: Schema): Batch = Batch(schema.types.map(t => Vec.nullVec(t, 0)).toArray, 0)

  /** A batch with no columns that still carries a row count (e.g. `select 1` input) */
  def ofRows(n: Int): Batch = Batch(Array.empty, n)

  /** Concatenate batches with the same column types */
  def concat(schema: Schema, batches: Seq[Batch]): Batch =
    if batches.size == 1 then
      batches.head
    else
      val total = batches.map(_.size).sum
      val cols  =
        Array.tabulate(schema.size) { c =>
          val b = VecBuilder(schema(c).ptype, total)
          batches.foreach { batch =>
            val v = batch.columns(c)
            var i = 0
            while i < batch.size do
              b.add(v.get(i))
              i += 1
          }
          b.result()
        }
      Batch(cols, total)

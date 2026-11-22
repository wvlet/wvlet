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
      // Positions are computed once and shared by every column
      val idx = new Array[Int](count)
      var j   = 0
      i = 0
      while i < size do
        if mask(i) then
          idx(j) = i
          j += 1
        i += 1
      gather(idx, count)

  def gather(idx: Array[Int], n: Int): Batch = Batch(columns.map(_.gather(idx, n)), n)

  def slice(from: Int, until: Int): Batch =
    val end = math.min(until, size)
    if from == 0 && end == size then
      this
    else
      Batch(columns.map(_.slice(from, end)), math.max(0, end - from))

  def select(indexes: Array[Int]): Batch = Batch(indexes.map(columns), size)

  override def toString: String = s"Batch(${size} rows, ${numColumns} columns)"

end Batch

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
        Array.tabulate(schema.size)(c =>
          concatVecs(schema(c).ptype, batches.map(_.columns(c)), total)
        )
      Batch(cols, total)

  /** Vectors of one column concatenated: typed arrays are copied as is, others go through boxing */
  private def concatVecs(ptype: PType, vs: Seq[Vec], total: Int): Vec =
    inline def nullsOf(): Array[Boolean] =
      if vs.forall(_.nulls == null) then
        null
      else
        val o   = new Array[Boolean](total)
        var off = 0
        vs.foreach { v =>
          if v.nulls != null then
            System.arraycopy(v.nulls, 0, o, off, v.size)
          off += v.size
        }
        o
    inline def copy[A: scala.reflect.ClassTag](values: Vec => Array[A]): Array[A] =
      val o   = new Array[A](total)
      var off = 0
      vs.foreach { v =>
        System.arraycopy(values(v), 0, o, off, v.size)
        off += v.size
      }
      o
    val first = vs.headOption.orNull
    first match
      case l: LongVec
          if l.ptype == ptype && vs.forall(v => v.isInstanceOf[LongVec] && v.ptype == ptype) =>
        LongVec(l.ptype, copy(_.asInstanceOf[LongVec].values), nullsOf())
      case i: IntVec
          if i.ptype == ptype && vs.forall(v => v.isInstanceOf[IntVec] && v.ptype == ptype) =>
        IntVec(i.ptype, copy(_.asInstanceOf[IntVec].values), nullsOf())
      case _: DoubleVec if ptype == PType.Float64 && vs.forall(_.isInstanceOf[DoubleVec]) =>
        DoubleVec(copy(_.asInstanceOf[DoubleVec].values), nullsOf())
      case _: StrVec if ptype == PType.Str && vs.forall(_.isInstanceOf[StrVec]) =>
        StrVec(copy(_.asInstanceOf[StrVec].values), nullsOf())
      case _: BoolVec if ptype == PType.Bool && vs.forall(_.isInstanceOf[BoolVec]) =>
        BoolVec(copy(_.asInstanceOf[BoolVec].values), nullsOf())
      case _ =>
        val b = VecBuilder(ptype, total)
        vs.foreach { v =>
          var i = 0
          while i < v.size do
            b.add(v.get(i))
            i += 1
        }
        b.result()

  end concatVecs

end Batch

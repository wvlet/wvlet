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

/**
  * A set of equally sized column vectors.
  *
  * A column can be deferred: a source vector and the row positions (an index array, -1 for NULL) to
  * take from it, shared by the columns that came through the same [[take]]. A deferred column is
  * gathered on its first [[column]] read and cached; [[columns]] and [[compact]] gather all of
  * them. Taking rows of deferred columns composes the index arrays, so a chain of filters and joins
  * gathers each column once, at its reader. Deferred batches stay inside the operator pipeline that
  * made them: buffered batches ([[wvlet.lang.engine.exec.Operator.drain]], [[Batch.concat]]) are
  * compacted, so the cache is never shared across threads.
  */
final class Batch private (
    private val cols: Array[Vec],
    private var srcs: Array[Vec],
    private var sels: Array[Array[Int]],
    val size: Int
):
  def this(columns: Array[Vec], size: Int) = this(columns, null, null, size)

  def numColumns: Int = cols.length

  /** Whether some columns are not gathered yet */
  def isDeferred: Boolean = srcs != null

  /** The i-th column, gathered on first read when deferred */
  def column(i: Int): Vec =
    var v = cols(i)
    if v == null then
      v = srcs(i).gather(sels(i), size)
      cols(i) = v
    v

  /** All columns, gathered */
  def columns: Array[Vec] =
    compact()
    cols

  /** Gather the deferred columns and drop the references to their sources */
  def compact(): Batch =
    if srcs != null then
      var i = 0
      while i < cols.length do
        column(i)
        i += 1
      srcs = null
      sels = null
    this

  def isEmpty: Boolean        = size == 0
  def row(i: Int): Array[Any] = Array.tabulate(cols.length)(c => column(c).get(i))

  /** Positions of the true values of the mask, or null when every row is selected */
  private def maskPositions(mask: Array[Boolean]): Array[Int] =
    var count = 0
    var i     = 0
    while i < size do
      if mask(i) then
        count += 1
      i += 1
    if count == size then
      null
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
      idx

  /** Rows whose mask is true, gathered */
  def filter(mask: Array[Boolean]): Batch =
    val idx = maskPositions(mask)
    if idx == null then
      this
    else
      gather(idx, idx.length)

  /** Rows whose mask is true, with the columns deferred ([[take]]) */
  def takeWhere(mask: Array[Boolean]): Batch =
    val idx = maskPositions(mask)
    if idx == null then
      this
    else
      take(idx, idx.length)

  /** Rows at the given positions (-1 for a row of NULLs), gathered */
  def gather(idx: Array[Int], n: Int): Batch = take(idx, n).compact()

  /**
    * Rows at the given positions (-1 for a row of NULLs), with every column deferred: no column is
    * copied until it is read. `idx` must not be modified afterwards
    */
  def take(idx: Array[Int], n: Int): Batch =
    val m = cols.length
    if m == 0 then
      return Batch(Array.empty[Vec], n)
    val newSrcs = new Array[Vec](m)
    val newSels = new Array[Array[Int]](m)
    // Index arrays composed so far, by identity of the inner one (a batch has few distinct ones)
    var inner: List[(Array[Int], Array[Int])] = Nil
    var i                                     = 0
    while i < m do
      val v = cols(i)
      if v != null then
        newSrcs(i) = v
        newSels(i) = idx
      else
        val sel = sels(i)
        newSrcs(i) = srcs(i)
        newSels(i) =
          inner.find(_._1 eq sel) match
            case Some((_, composed)) =>
              composed
            case None =>
              val composed = Batch.compose(sel, idx, n)
              inner = (sel, composed) :: inner
              composed
      i += 1
    Batch(new Array[Vec](m), newSrcs, newSels, n)

  end take

  def slice(from: Int, until: Int): Batch =
    val end = math.min(until, size)
    if from == 0 && end == size then
      this
    else if srcs != null then
      val n = math.max(0, end - from)
      take(Array.tabulate(n)(from + _), n)
    else
      Batch(cols.map(_.slice(from, end)), math.max(0, end - from))

  /** The columns at the given positions, in that order (deferred columns stay deferred) */
  def select(indexes: Array[Int]): Batch =
    if srcs == null then
      Batch(indexes.map(cols), size)
    else
      Batch(indexes.map(cols), indexes.map(srcs), indexes.map(sels), size)

  /** The columns of this batch followed by those of `other`, which has the same row count */
  def ++(other: Batch): Batch =
    require(size == other.size, s"Row counts differ: ${size} and ${other.size}")
    if srcs == null && other.srcs == null then
      Batch(cols ++ other.cols, size)
    else
      def srcsOf(b: Batch): Array[Vec] =
        if b.srcs == null then
          new Array[Vec](b.cols.length)
        else
          b.srcs
      def selsOf(b: Batch): Array[Array[Int]] =
        if b.sels == null then
          new Array[Array[Int]](b.cols.length)
        else
          b.sels
      Batch(cols ++ other.cols, srcsOf(this) ++ srcsOf(other), selsOf(this) ++ selsOf(other), size)

  override def toString: String = s"Batch(${size} rows, ${numColumns} columns)"

end Batch

object Batch:
  val DefaultSize = 4096

  def empty(schema: Schema): Batch = Batch(schema.types.map(t => Vec.nullVec(t, 0)).toArray, 0)

  /** A batch with no columns that still carries a row count (e.g. `select 1` input) */
  def ofRows(n: Int): Batch = Batch(Array.empty, n)

  /** `outer` positions mapped through `inner` positions; -1 (a row of NULLs) stays -1 */
  private def compose(inner: Array[Int], outer: Array[Int], n: Int): Array[Int] =
    val out = new Array[Int](n)
    var i   = 0
    while i < n do
      val k = outer(i)
      out(i) =
        if k < 0 then
          -1
        else
          inner(k)
      i += 1
    out

  /** Concatenate batches with the same column types */
  def concat(schema: Schema, batches: Seq[Batch]): Batch =
    if batches.size == 1 then
      batches.head.compact()
    else
      val total = batches.map(_.size).sum
      val cols  =
        Array.tabulate(schema.size)(c =>
          concatVecs(schema(c).ptype, batches.map(_.columns(c)), total)
        )
      Batch(cols, total)

  /** Vectors of one column concatenated: typed arrays are copied as is, others go through boxing */
  private[engine] def concatVecs(ptype: PType, vs: Seq[Vec], total: Int): Vec =
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

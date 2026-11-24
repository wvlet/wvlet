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
package wvlet.lang.engine.expr

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.vector.*

import java.math.BigDecimal as JBigDecimal
import java.math.MathContext
import java.math.RoundingMode

/**
  * A compiled, bound expression evaluated one batch at a time. `sig` is a structural signature used
  * to match an expression against grouping keys and already computed columns
  */
trait VExpr:
  def ptype: PType
  def eval(b: Batch): Vec
  def sig: String
  def children: Seq[VExpr] = Nil

  /** True when the value doesn't depend on the input rows */
  def isConstant: Boolean = children.nonEmpty && children.forall(_.isConstant)

  override def toString: String = sig

object VExpr:
  /**
    * A constant as signature text: as is, or quoted SQL-style when its text could read as NULL, an
    * empty value, or more than one value or expression (`'x:string),lit(y'` against `'x', 'y'`).
    * Passes match conditions by signature (FactorOr, PushDownFilters), so the text must tell
    * different values apart. Lists and maps print their elements the same way
    */
  def valueSig(v: Any): String =
    v match
      case s: String if s.isEmpty || s == "null" || s.exists(ch => ",'()[]{}:".indexOf(ch) >= 0) =>
        s"'${s.replace("'", "''")}'"
      case xs: Seq[?] =>
        xs.map(valueSig).mkString("[", ",", "]")
      case m: Map[?, ?] =>
        m.map((k, x) => s"${valueSig(k)}:${valueSig(x)}").mkString("{", ",", "}")
      case other =>
        String.valueOf(other)

/** A reference to an input column */
case class ColRef(index: Int, ptype: PType, name: String) extends VExpr:
  def eval(b: Batch): Vec          = b.column(index)
  def sig: String                  = s"#${index}"
  override def isConstant: Boolean = false

case class Const(value: Any, ptype: PType) extends VExpr:
  // Vectors are immutable, so the last vector built is reused for batches of the same size
  @volatile
  private var cached: Vec = null

  def eval(b: Batch): Vec =
    val c = cached
    if c != null && c.size == b.size then
      c
    else
      val v = Vec.constant(ptype, value, b.size)
      cached = v
      v

  def sig: String                  = s"lit(${VExpr.valueSig(value)}:${ptype})"
  override def isConstant: Boolean = true

/** Evaluate a row-wise function: the fallback path for functions without a vector kernel */
case class RowFn(
    name: String,
    args: Seq[VExpr],
    ptype: PType,
    f: Array[Any] => Any,
    nullSafe: Boolean = false
) extends VExpr:
  override def children: Seq[VExpr] = args
  def sig: String                   = s"${name}(${args.map(_.sig).mkString(",")})"
  def eval(b: Batch): Vec           =
    val vs  = args.map(_.eval(b)).toArray
    val out = VecBuilder(ptype, b.size)
    val row = new Array[Any](vs.length)
    var i   = 0
    while i < b.size do
      var hasNull = false
      var j       = 0
      while j < vs.length do
        val v = vs(j).get(i)
        if v == null then
          hasNull = true
        row(j) = v
        j += 1
      if hasNull && !nullSafe then
        out.addNull()
      else
        out.add(f(row))
      i += 1
    out.result()

case class CastExpr(child: VExpr, ptype: PType, tryCast: Boolean = false) extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def sig: String                   = s"cast(${child.sig} as ${ptype})"
  def eval(b: Batch): Vec           =
    val v = child.eval(b)
    if v.ptype == ptype then
      v
    else
      Kernels.cast(v, ptype, tryCast)

enum ArithOp(val symbol: String):
  case Add    extends ArithOp("+")
  case Sub    extends ArithOp("-")
  case Mul    extends ArithOp("*")
  case Div    extends ArithOp("/")
  case IntDiv extends ArithOp("//")
  case Mod    extends ArithOp("%")

case class Arith(op: ArithOp, left: VExpr, right: VExpr, ptype: PType) extends VExpr:
  override def children: Seq[VExpr] = Seq(left, right)
  def sig: String                   = s"(${left.sig}${op.symbol}${right.sig})"
  def eval(b: Batch): Vec           = Kernels.arith(op, left.eval(b), right.eval(b), ptype)

case class Negate(child: VExpr) extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def ptype: PType                  = child.ptype
  def sig: String                   = s"-(${child.sig})"
  def eval(b: Batch): Vec           = Kernels.negate(child.eval(b))

enum CmpOp(val symbol: String):
  case Eq extends CmpOp("=")
  case Ne extends CmpOp("!=")
  case Lt extends CmpOp("<")
  case Le extends CmpOp("<=")
  case Gt extends CmpOp(">")
  case Ge extends CmpOp(">=")

  /** The operator with its operands swapped: `a < b` is `b > a` */
  def flip: CmpOp =
    this match
      case Lt =>
        Gt
      case Le =>
        Ge
      case Gt =>
        Lt
      case Ge =>
        Le
      case other =>
        other

  def test(c: Int): Boolean =
    this match
      case Eq =>
        c == 0
      case Ne =>
        c != 0
      case Lt =>
        c < 0
      case Le =>
        c <= 0
      case Gt =>
        c > 0
      case Ge =>
        c >= 0

end CmpOp

/** Comparison of two operands already cast to the same type */
case class Compare(op: CmpOp, left: VExpr, right: VExpr) extends VExpr:
  override def children: Seq[VExpr] = Seq(left, right)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"(${left.sig}${op.symbol}${right.sig})"
  // Constant operands are evaluated once and compared as scalars
  private lazy val leftScalar: Option[Any]  = Kernels.scalarOf(left)
  private lazy val rightScalar: Option[Any] = Kernels.scalarOf(right)

  def eval(b: Batch): Vec =
    rightScalar match
      case Some(v) if leftScalar.isEmpty =>
        Kernels.compareScalar(op, left.eval(b), v, right.ptype)
      case _ =>
        leftScalar match
          case Some(v) if rightScalar.isEmpty =>
            Kernels.compareScalar(op.flip, right.eval(b), v, left.ptype)
          case _ =>
            Kernels.compare(op, left.eval(b), right.eval(b))

/** IS [NOT] DISTINCT FROM: NULL-safe equality */
case class NullSafeDistinct(left: VExpr, right: VExpr, negate: Boolean) extends VExpr:
  override def children: Seq[VExpr] = Seq(left, right)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"distinct_from(${left.sig},${right.sig},${negate})"
  def eval(b: Batch): Vec           =
    val l   = left.eval(b)
    val r   = right.eval(b)
    val out = new Array[Boolean](b.size)
    var i   = 0
    while i < b.size do
      val a        = l.get(i)
      val c        = r.get(i)
      val distinct =
        if a == null && c == null then
          false
        else if a == null || c == null then
          true
        else
          ValueOps.compare(a, c) != 0
      out(i) = distinct != negate
      i += 1
    BoolVec(out, null)

case class AndExpr(left: VExpr, right: VExpr) extends VExpr:
  override def children: Seq[VExpr] = Seq(left, right)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"(${left.sig} and ${right.sig})"
  def eval(b: Batch): Vec           = Kernels.and(left.eval(b), right.eval(b))

case class OrExpr(left: VExpr, right: VExpr) extends VExpr:
  override def children: Seq[VExpr] = Seq(left, right)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"(${left.sig} or ${right.sig})"
  def eval(b: Batch): Vec           = Kernels.or(left.eval(b), right.eval(b))

case class NotExpr(child: VExpr) extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"not(${child.sig})"
  def eval(b: Batch): Vec           =
    val v = Kernels.asBool(child.eval(b))
    val n = v.size
    val o = new Array[Boolean](n)
    var i = 0
    while i < n do
      o(i) = !v.values(i)
      i += 1
    BoolVec(o, v.nulls)

case class IsNullExpr(child: VExpr, negate: Boolean) extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"is_null(${child.sig},${negate})"
  def eval(b: Batch): Vec           =
    val v = child.eval(b)
    val o = new Array[Boolean](b.size)
    var i = 0
    while i < b.size do
      o(i) = v.isNull(i) != negate
      i += 1
    BoolVec(o, null)

/**
  * `x IN (constants...)`, with SQL NULL semantics: unknown for a NULL `x`, or for a missing `x`
  * when a value is NULL. Over no values at all (an empty subquery), IN is FALSE and NOT IN TRUE,
  * NULL `x` included
  */
case class InList(child: VExpr, values: Set[Any], hasNullValue: Boolean, negate: Boolean)
    extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def ptype: PType                  = PType.Bool
  // Passes match conditions by signature (FactorOr, PushDownFilters), so a NULL in the list is
  // marked (it changes the value where no value matches), and values that could read as another
  // list are quoted: `in (1)` vs `in (1, null)`, and `in ('x,y')` vs `in ('x', 'y')`
  def sig: String =
    val nullMarker =
      if hasNullValue then
        Iterator("null")
      else
        Iterator.empty
    val vs = values.iterator.map(VExpr.valueSig) ++ nullMarker
    s"in(${child.sig},${vs.mkString(",")},${negate})"

  def eval(b: Batch): Vec =
    val v                     = child.eval(b)
    val o                     = new Array[Boolean](b.size)
    var nulls: Array[Boolean] = null
    val noValues              = values.isEmpty && !hasNullValue
    var i                     = 0
    while i < b.size do
      val x = v.get(i)
      if x == null then
        if noValues then
          o(i) = negate
        else
          if nulls == null then
            nulls = new Array[Boolean](b.size)
          nulls(i) = true
      else
        val found = values.contains(x)
        if !found && hasNullValue then
          if nulls == null then
            nulls = new Array[Boolean](b.size)
          nulls(i) = true
        else
          o(i) = found != negate
      i += 1
    BoolVec(o, nulls)

end InList

/** `x IN (exprs...)` for non-constant lists: an OR of equalities */
case class InExprs(child: VExpr, list: Seq[VExpr], negate: Boolean) extends VExpr:
  override def children: Seq[VExpr] = child +: list
  def ptype: PType                  = PType.Bool
  def sig: String         = s"in_exprs(${child.sig},${list.map(_.sig).mkString(",")},${negate})"
  def eval(b: Batch): Vec =
    val ors: VExpr = list
      .map(e => Compare(CmpOp.Eq, child, e): VExpr)
      .reduce((a, c) => OrExpr(a, c))
    val r = ors.eval(b)
    if negate then
      NotExpr(Materialized(r)).eval(b)
    else
      r

/** An already evaluated vector (used for composing kernels) */
case class Materialized(v: Vec) extends VExpr:
  def ptype: PType        = v.ptype
  def eval(b: Batch): Vec = v
  def sig: String         = s"materialized@${System.identityHashCode(v)}"

/** CASE WHEN c1 THEN r1 ... ELSE e END; results are cast to ptype */
case class CaseWhen(whens: Seq[(VExpr, VExpr)], elseExpr: Option[VExpr], ptype: PType)
    extends VExpr:
  override def children: Seq[VExpr] = whens.flatMap(w => Seq(w._1, w._2)) ++ elseExpr.toSeq
  def sig: String                   =
    s"case(${whens.map((c, r) => s"${c.sig}->${r.sig}").mkString(",")};${elseExpr.map(_.sig)})"

  def eval(b: Batch): Vec =
    val n = b.size
    val m = whens.size
    // The branch each row takes; m is the ELSE branch
    val chosen  = Vec.filledInts(n, m)
    val picked  = new Array[Boolean](m + 1)
    var pending = n
    var k       = 0
    while k < m && pending > 0 do
      val c  = Kernels.asBool(whens(k)._1.eval(b))
      val cv = c.values
      val cn = c.nulls
      var i  = 0
      while i < n do
        if chosen(i) == m && cv(i) && (cn == null || !cn(i)) then
          chosen(i) = k
          pending -= 1
          picked(k) = true
        i += 1
      k += 1
    picked(m) = pending > 0
    // Only the branches some row takes are evaluated. A null source yields NULL (no ELSE)
    val srcs = new Array[Vec](m + 1)
    k = 0
    while k < m do
      if picked(k) then
        srcs(k) = CaseWhen.evalBranch(whens(k)._2, b, ptype)
      k += 1
    if picked(m) then
      elseExpr.foreach(e => srcs(m) = CaseWhen.evalBranch(e, b, ptype))
    CaseWhen.fill(ptype, n, chosen, srcs, picked)

  end eval

end CaseWhen

object CaseWhen:
  /** A branch's values cast to ptype, or null for a NULL literal (which yields NULL rows) */
  private[expr] def evalBranch(e: VExpr, b: Batch, ptype: PType): Vec =
    e match
      case Const(null, _) =>
        null
      case _ =>
        Kernels.castTo(e.eval(b), ptype)

  /**
    * The output of a CASE WHEN: row i is row i of `srcs(chosen(i))`. Values are copied between
    * arrays of the same physical type, without boxing, when every taken source has the output
    * layout; other layouts (wide decimals, nested values) go through a row-wise builder
    */
  private[expr] def fill(
      ptype: PType,
      n: Int,
      chosen: Array[Int],
      srcs: Array[Vec],
      picked: Array[Boolean]
  ): Vec =
    var first: Vec = null
    var nullable   = false
    var typed      = true
    var taken      = 0
    var k          = 0
    while k < srcs.length do
      if picked(k) then
        taken += 1
        val s = srcs(k)
        if s == null then
          nullable = true
        else
          if first == null then
            first = s
          if s.nulls != null then
            nullable = true
          typed = typed && hasLayout(s, ptype)
      k += 1
    if first == null then
      Vec.nullVec(ptype, n)
    else if taken == 1 then
      // Every row takes one branch: its vector (already cast to ptype) is the result
      first
    else if !typed then
      rowFill(ptype, n, chosen, srcs)
    else
      val nulls =
        if nullable then
          fillNulls(n, chosen, srcs)
        else
          null
      // The value slot of a NULL row is unspecified, so a missing source reads any taken one
      def src(j: Int): Vec =
        if srcs(j) == null then
          first
        else
          srcs(j)
      first match
        case _: BoolVec =>
          val vs  = Array.tabulate(srcs.length)(src(_).asInstanceOf[BoolVec].values)
          val out = new Array[Boolean](n)
          var i   = 0
          while i < n do
            out(i) = vs(chosen(i))(i)
            i += 1
          BoolVec(out, nulls)
        case _: IntVec =>
          val vs  = Array.tabulate(srcs.length)(src(_).asInstanceOf[IntVec].values)
          val out = new Array[Int](n)
          var i   = 0
          while i < n do
            out(i) = vs(chosen(i))(i)
            i += 1
          IntVec(ptype, out, nulls)
        case _: LongVec =>
          val vs  = Array.tabulate(srcs.length)(src(_).asInstanceOf[LongVec].values)
          val out = new Array[Long](n)
          var i   = 0
          while i < n do
            out(i) = vs(chosen(i))(i)
            i += 1
          LongVec(ptype, out, nulls)
        case _: DoubleVec =>
          val vs  = Array.tabulate(srcs.length)(src(_).asInstanceOf[DoubleVec].values)
          val out = new Array[Double](n)
          var i   = 0
          while i < n do
            out(i) = vs(chosen(i))(i)
            i += 1
          DoubleVec(out, nulls)
        case _: StrVec =>
          val vs  = Array.tabulate(srcs.length)(src(_).asInstanceOf[StrVec].values)
          val out = new Array[String](n)
          var i   = 0
          while i < n do
            out(i) = vs(chosen(i))(i)
            i += 1
          StrVec(out, nulls)
        case _ =>
          rowFill(ptype, n, chosen, srcs)
      end match
    end if

  end fill

  /** True when the vector stores values in the typed array of the output type */
  private def hasLayout(v: Vec, ptype: PType): Boolean =
    v match
      case _: BoolVec =>
        ptype == PType.Bool
      case i: IntVec =>
        i.ptype == ptype
      // Long-backed decimals hold unscaled values at the output scale only for the same type
      case l: LongVec =>
        l.ptype == ptype
      case _: DoubleVec =>
        ptype == PType.Float64
      case _: StrVec =>
        ptype == PType.Str
      case _ =>
        false

  private def fillNulls(n: Int, chosen: Array[Int], srcs: Array[Vec]): Array[Boolean] =
    val nullArrs = new Array[Array[Boolean]](srcs.length)
    val missing  = new Array[Boolean](srcs.length)
    var k        = 0
    while k < srcs.length do
      if srcs(k) == null then
        missing(k) = true
      else
        nullArrs(k) = srcs(k).nulls
      k += 1
    val out = new Array[Boolean](n)
    var any = false
    var i   = 0
    while i < n do
      val c      = chosen(i)
      val ns     = nullArrs(c)
      val isNull = missing(c) || (ns != null && ns(i))
      out(i) = isNull
      any = any | isNull
      i += 1
    if any then
      out
    else
      null

  private def rowFill(ptype: PType, n: Int, chosen: Array[Int], srcs: Array[Vec]): Vec =
    val out = VecBuilder(ptype, n)
    var i   = 0
    while i < n do
      val s = srcs(chosen(i))
      if s == null then
        out.addNull()
      else
        out.add(s.get(i))
      i += 1
    out.result()

end CaseWhen

case class Coalesce(args: Seq[VExpr], ptype: PType) extends VExpr:
  override def children: Seq[VExpr] = args
  def sig: String                   = s"coalesce(${args.map(_.sig).mkString(",")})"

  // NULL literals never supply a value
  private val inputs: Array[VExpr] =
    args
      .filter {
        case Const(null, _) =>
          false
        case _ =>
          true
      }
      .toArray

  def eval(b: Batch): Vec =
    val n = b.size
    val m = inputs.length
    // The argument each row takes; m means every argument is NULL
    val chosen  = Vec.filledInts(n, m)
    val picked  = new Array[Boolean](m + 1)
    val srcs    = new Array[Vec](m + 1)
    var pending = n
    var k       = 0
    // Later arguments are evaluated only while some rows are still NULL
    while k < m && pending > 0 do
      val v  = Kernels.castTo(inputs(k).eval(b), ptype)
      val ns = v.nulls
      srcs(k) = v
      var i = 0
      while i < n do
        if chosen(i) == m && (ns == null || !ns(i)) then
          chosen(i) = k
          pending -= 1
          picked(k) = true
        i += 1
      k += 1
    picked(m) = pending > 0
    CaseWhen.fill(ptype, n, chosen, srcs, picked)

  end eval

end Coalesce

case class LikeExpr(
    child: VExpr,
    pattern: String,
    negate: Boolean,
    caseInsensitive: Boolean = false,
    escape: Option[Char] = None
) extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def ptype: PType                  = PType.Bool
  // The escape character changes what the pattern matches: `'a!%' escape '!'` matches `a%` only
  def sig: String =
    val esc = escape.map(e => s",escape=${VExpr.valueSig(e.toString)}").getOrElse("")
    s"like(${child.sig},${VExpr.valueSig(pattern)},${negate},${caseInsensitive}${esc})"

  private val matcher: String => Boolean = LikeMatcher.compile(pattern, caseInsensitive, escape)
  def eval(b: Batch): Vec                =
    val v = Kernels.asStrVec(child.eval(b))
    val o = new Array[Boolean](b.size)
    var i = 0
    while i < b.size do
      if !v.isNull(i) then
        o(i) = matcher(v.values(i)) != negate
      i += 1
    BoolVec(o, v.nulls)

object LikeMatcher:
  def compile(pattern: String, caseInsensitive: Boolean, escape: Option[Char]): String => Boolean =
    val hasEscape = escape.isDefined
    // Fast paths for prefix/suffix/contains patterns
    if !hasEscape && !caseInsensitive && !pattern.contains('_') then
      val parts = pattern.split("%", -1)
      if parts.length == 1 then
        return (s: String) => s == pattern
      else if parts.length == 2 && parts(0).isEmpty && parts(1).isEmpty then
        return (_: String) => true
      else if parts.length == 2 && parts(1).isEmpty then
        val p = parts(0)
        return (s: String) => s.startsWith(p)
      else if parts.length == 2 && parts(0).isEmpty then
        val p = parts(1)
        return (s: String) => s.endsWith(p)
      else if parts.length == 3 && parts(0).isEmpty && parts(2).isEmpty then
        val p = parts(1)
        return (s: String) => s.contains(p)
      else
        // Sequence of fragments
        val first = parts.head
        val last  = parts.last
        val mids  = parts.slice(1, parts.length - 1)
        return (s: String) =>
          if !s.startsWith(first) || !s.endsWith(last) || s.length < first.length + last.length then
            false
          else
            var pos = first.length
            val end = s.length - last.length
            var ok  = true
            var k   = 0
            while ok && k < mids.length do
              val m = mids(k)
              val i = s.indexOf(m, pos)
              if i < 0 || i + m.length > end then
                ok = false
              else
                pos = i + m.length
              k += 1
            ok
      end if
    end if
    val sb = StringBuilder()
    if caseInsensitive then
      sb.append("(?is)")
    else
      sb.append("(?s)")
    var i = 0
    while i < pattern.length do
      val c = pattern.charAt(i)
      if hasEscape && c == escape.get && i + 1 < pattern.length then
        sb.append(java.util.regex.Pattern.quote(pattern.charAt(i + 1).toString))
        i += 1
      else if c == '%' then
        sb.append(".*")
      else if c == '_' then
        sb.append(".")
      else
        sb.append(java.util.regex.Pattern.quote(c.toString))
      i += 1
    val re = java.util.regex.Pattern.compile(sb.toString)
    (s: String) => re.matcher(s).matches()

  end compile

end LikeMatcher

object Kernels:
  private val DivContext = MathContext(38, RoundingMode.HALF_UP)

  def orNulls(a: Array[Boolean], b: Array[Boolean], n: Int): Array[Boolean] =
    // Null arrays are immutable, so a shared one (x op x, or two compares of one column) is reused
    if a == null || (a eq b) then
      b
    else if b == null then
      a
    else
      val o = new Array[Boolean](n)
      var i = 0
      while i < n do
        o(i) = a(i) || b(i)
        i += 1
      o

  def asBool(v: Vec): BoolVec =
    v match
      case b: BoolVec =>
        b
      case other =>
        castTo(other, PType.Bool).asInstanceOf[BoolVec]

  def asStrVec(v: Vec): StrVec =
    v match
      case s: StrVec =>
        s
      case other =>
        castTo(other, PType.Str).asInstanceOf[StrVec]

  def asLongs(v: Vec): Array[Long] =
    v match
      case l: LongVec if !l.ptype.isDecimal =>
        l.values
      case i: IntVec =>
        val o = new Array[Long](i.size)
        var k = 0
        while k < o.length do
          o(k) = i.values(k)
          k += 1
        o
      case other =>
        castTo(other, PType.Int64).asInstanceOf[LongVec].values

  def asDoubles(v: Vec): Array[Double] =
    v match
      case d: DoubleVec =>
        d.values
      case i: IntVec =>
        val o = new Array[Double](i.size)
        var k = 0
        while k < o.length do
          o(k) = i.values(k)
          k += 1
        o
      case l: LongVec if !l.ptype.isDecimal =>
        val o = new Array[Double](l.size)
        var k = 0
        while k < o.length do
          o(k) = l.values(k).toDouble
          k += 1
        o
      // Long-backed decimals: unscaled / 10^scale when that rounds correctly, else via BigDecimal
      case l: LongVec =>
        val o = decimalToDoubles(l.values, decimalScale(l))
        if o != null then
          o
        else
          rowDoubles(l)
      case other =>
        rowDoubles(other)

  private def rowDoubles(v: Vec): Array[Double] =
    val o = new Array[Double](v.size)
    var k = 0
    while k < o.length do
      val x = v.get(k)
      if x != null then
        o(k) = ValueOps.toDouble(x)
      k += 1
    o

  private val Pow10: Array[Long] =
    val a = new Array[Long](19)
    a(0) = 1L
    var i = 1
    while i < 19 do
      a(i) = a(i - 1) * 10L
      i += 1
    a

  /**
    * Unscaled Long values of a numeric vector at the given scale, or null when a value does not fit
    * in a Long
    */
  def asUnscaled(v: Vec, scale: Int): Array[Long] =
    v match
      case l: LongVec =>
        l.ptype match
          case PType.Decimal(_, s) =>
            if s == scale then
              l.values
            else if s < scale then
              val diff = scale - s
              if diff > 18 then
                return null
              val m = Pow10(diff)
              val o = new Array[Long](l.size)
              var k = 0
              while k < o.length do
                val x = l.values(k)
                val r = x * m
                if x != 0 && (r / m != x) then
                  return null
                o(k) = r
                k += 1
              o
            else
              null
          case _ =>
            scaleIntegral(l.values, scale)
      case i: IntVec =>
        scaleIntegral(asLongs(i), scale)
      case _ =>
        null

  private def scaleIntegral(values: Array[Long], scale: Int): Array[Long] =
    if scale == 0 then
      values
    else if scale > 18 then
      null
    else
      val m = Pow10(scale)
      val o = new Array[Long](values.length)
      var k = 0
      while k < o.length do
        val x = values(k)
        val r = x * m
        if x != 0 && (r / m != x) then
          return null
        o(k) = r
        k += 1
      o

  def asBig(v: Vec): Array[JBigDecimal] =
    val o = new Array[JBigDecimal](v.size)
    var k = 0
    while k < o.length do
      val x = v.get(k)
      if x != null then
        o(k) = ValueOps.toBigDecimal(x)
      k += 1
    o

  def castTo(v: Vec, to: PType): Vec =
    if v.ptype == to then
      v
    else
      cast(v, to, tryCast = false)

  def cast(v: Vec, to: PType, tryCast: Boolean): Vec =
    (v, to) match
      // Every value of an untyped NULL vector is NULL
      case _ if v.ptype == PType.Null =>
        Vec.nullVec(to, v.size)
      case (l: LongVec, d: PType.Decimal) if l.ptype.isDecimal && decimalScale(l) > d.scale =>
        val u = scaleDown(l.values, decimalScale(l) - d.scale)
        if u != null then
          LongVec(d, u, l.nulls)
        else
          genericCast(v, to, tryCast)
      case (l: LongVec, PType.Float64) if l.ptype.isDecimal =>
        val o = decimalToDoubles(l.values, decimalScale(l))
        if o != null then
          DoubleVec(o, l.nulls)
        else
          genericCast(v, to, tryCast)
      case (i: IntVec, PType.Int64) if i.ptype == PType.Int32 =>
        LongVec(PType.Int64, asLongs(i), i.nulls)
      case (_: IntVec | _: LongVec, PType.Float64)
          if !v.ptype.isDecimal && v.ptype != PType.Date && !v.ptype.isTimestamp =>
        DoubleVec(asDoubles(v), v.nulls)
      case (_, d: PType.Decimal) if v.ptype.isIntegral || v.ptype.isDecimal =>
        val u = asUnscaled(v, d.scale)
        if u != null then
          LongVec(d, u, v.nulls)
        else
          genericCast(v, to, tryCast)
      case _ =>
        genericCast(v, to, tryCast)

  private def decimalScale(v: Vec): Int =
    v.ptype match
      case PType.Decimal(_, s) =>
        s
      case _ =>
        0

  /**
    * Unscaled values divided by 10^diff, rounding half away from zero like `ValueOps.rescale`
    * (HALF_UP), or null when 10^diff does not fit in a Long
    */
  private def scaleDown(values: Array[Long], diff: Int): Array[Long] =
    if diff > 18 then
      null
    else
      val m    = Pow10(diff)
      val half = m / 2
      val o    = new Array[Long](values.length)
      var k    = 0
      while k < o.length do
        val x = values(k)
        val q = x / m
        val r = x % m
        o(k) =
          if r >= half then
            q + 1
          else if -r >= half then
            q - 1
          else
            q
        k += 1
      o

  // Powers of ten exactly representable as doubles (10^22 is the largest)
  private val DoublePow10: Array[Double] = Array.tabulate(23)(i => s"1e${i}".toDouble)

  /**
    * Decimal values as doubles, or null when some |unscaled| exceeds 2^53. Below that bound both
    * the unscaled value and 10^scale are exact doubles, so one division rounds the decimal value
    * correctly, as `BigDecimal.doubleValue` does
    */
  private def decimalToDoubles(values: Array[Long], scale: Int): Array[Double] =
    if scale > 22 then
      null
    else
      val p     = DoublePow10(scale)
      val limit = 1L << 53
      val o     = new Array[Double](values.length)
      var k     = 0
      while k < o.length do
        val x = values(k)
        if x > limit || x < -limit then
          return null
        o(k) = x.toDouble / p
        k += 1
      o

  private def genericCast(v: Vec, to: PType, tryCast: Boolean): Vec =
    val b = VecBuilder(to, v.size)
    var i = 0
    while i < v.size do
      val x = v.get(i)
      if x == null then
        b.addNull()
      else if tryCast then
        try
          b.add(ValueOps.cast(x, v.ptype, to))
        catch
          case _: Exception =>
            b.addNull()
      else
        b.add(ValueOps.cast(x, v.ptype, to))
      i += 1
    b.result()

  def negate(v: Vec): Vec =
    v match
      case i: IntVec if i.ptype == PType.Int32 =>
        IntVec(PType.Int32, Vec.mapInt(i.values)(-_), i.nulls)
      case l: LongVec if !l.ptype.isTimestamp =>
        LongVec(l.ptype, Vec.mapLong(l.values)(-_), l.nulls)
      case d: DoubleVec =>
        DoubleVec(Vec.mapDouble(d.values)(-_), d.nulls)
      case o: ObjVec if o.ptype.isDecimal =>
        ObjVec(
          o.ptype,
          o.values
            .map(x =>
              if x == null then
                null
              else
                x.asInstanceOf[JBigDecimal].negate()
            ),
          o.nulls
        )
      case other =>
        throw StatusCode.INVALID_ARGUMENT.newException(s"Cannot negate ${other.ptype}")

  def arith(op: ArithOp, l: Vec, r: Vec, out: PType): Vec =
    val n     = l.size
    val nulls = orNulls(l.nulls, r.nulls, n)
    out match
      case PType.Int64 | PType.Int32 =>
        val a           = asLongs(l)
        val b           = asLongs(r)
        val o           = new Array[Long](n)
        var resultNulls = nulls
        var i           = 0
        op match
          case ArithOp.Add =>
            while i < n do
              o(i) = Math.addExact(a(i), b(i))
              i += 1
          case ArithOp.Sub =>
            while i < n do
              o(i) = Math.subtractExact(a(i), b(i))
              i += 1
          case ArithOp.Mul =>
            while i < n do
              o(i) = Math.multiplyExact(a(i), b(i))
              i += 1
          case ArithOp.IntDiv | ArithOp.Div | ArithOp.Mod =>
            val mod = op == ArithOp.Mod
            while i < n do
              if nulls == null || !nulls(i) then
                if b(i) == 0 then
                  if resultNulls eq nulls then
                    resultNulls =
                      if nulls == null then
                        new Array[Boolean](n)
                      else
                        nulls.clone()
                  resultNulls(i) = true
                else if mod then
                  o(i) = a(i) % b(i)
                else
                  o(i) = a(i) / b(i)
              i += 1
        end match
        if out == PType.Int32 then
          IntVec(PType.Int32, o.map(_.toInt), resultNulls)
        else
          LongVec(PType.Int64, o, resultNulls)
      case PType.Float64 =>
        val a           = asDoubles(l)
        val b           = asDoubles(r)
        val o           = new Array[Double](n)
        var resultNulls = nulls
        var i           = 0
        op match
          case ArithOp.Add =>
            while i < n do
              o(i) = a(i) + b(i)
              i += 1
          case ArithOp.Sub =>
            while i < n do
              o(i) = a(i) - b(i)
              i += 1
          case ArithOp.Mul =>
            while i < n do
              o(i) = a(i) * b(i)
              i += 1
          case ArithOp.Div | ArithOp.IntDiv | ArithOp.Mod =>
            // Decided once: comparing enum cases calls equals, which costs a call per row
            val div    = op == ArithOp.Div
            val intDiv = op == ArithOp.IntDiv
            while i < n do
              if b(i) == 0.0 then
                if resultNulls eq nulls then
                  resultNulls =
                    if nulls == null then
                      new Array[Boolean](n)
                    else
                      nulls.clone()
                resultNulls(i) = true
              else if div then
                o(i) = a(i) / b(i)
              else if intDiv then
                o(i) = math.floor(a(i) / b(i))
              else
                o(i) = a(i) % b(i)
              i += 1
        end match
        DoubleVec(o, resultNulls)
      case d: PType.Decimal =>
        decimalArith(op, l, r, d, nulls)
      case _ =>
        throw StatusCode
          .INVALID_ARGUMENT
          .newException(s"Unsupported arithmetic ${l.ptype} ${op.symbol} ${r.ptype}")
    end match

  end arith

  private def decimalArith(
      op: ArithOp,
      l: Vec,
      r: Vec,
      out: PType.Decimal,
      nulls: Array[Boolean]
  ): Vec =
    val n = l.size
    // Fast path on unscaled longs
    val fast: Vec =
      op match
        case ArithOp.Add | ArithOp.Sub =>
          val a = asUnscaled(l, out.scale)
          val b = asUnscaled(r, out.scale)
          if a == null || b == null then
            null
          else
            val o  = new Array[Long](n)
            var ok = true
            var i  = 0
            // Decided once: comparing enum cases calls equals, which costs a call per row
            if op == ArithOp.Add then
              while ok && i < n do
                val x = a(i)
                val y = b(i)
                val z = x + y
                // Overflow iff both operands have the same sign and the result's sign differs
                if ((x ^ z) & (y ^ z)) < 0 then
                  ok = false
                o(i) = z
                i += 1
            else
              while ok && i < n do
                val x = a(i)
                val y = b(i)
                val z = x - y
                if ((x ^ y) & (x ^ z)) < 0 then
                  ok = false
                o(i) = z
                i += 1
            if ok then
              LongVec(out, o, nulls)
            else
              null
          end if
        case ArithOp.Mul if decimalScale(l) + decimalScale(r) == out.scale =>
          val a = asUnscaled(l, decimalScale(l))
          val b = asUnscaled(r, decimalScale(r))
          if a == null || b == null then
            null
          else
            val o  = new Array[Long](n)
            var ok = true
            var i  = 0
            while ok && i < n do
              val x  = a(i)
              val y  = b(i)
              val lo = x * y
              if x != 0 && (lo / x != y || (x == -1 && y == Long.MinValue)) then
                ok = false
              o(i) = lo
              i += 1
            if ok then
              LongVec(out, o, nulls)
            else
              null
        case _ =>
          null
    if fast != null then
      fast
    else
      val a           = asBig(l)
      val b           = asBig(r)
      val o           = new Array[AnyRef](n)
      var resultNulls = nulls
      var i           = 0
      while i < n do
        if (nulls == null || !nulls(i)) && a(i) != null && b(i) != null then
          val x              = a(i)
          val y              = b(i)
          val z: JBigDecimal =
            op match
              case ArithOp.Add =>
                x.add(y)
              case ArithOp.Sub =>
                x.subtract(y)
              case ArithOp.Mul =>
                x.multiply(y)
              case ArithOp.Div | ArithOp.IntDiv | ArithOp.Mod =>
                if y.signum() == 0 then
                  null
                else
                  op match
                    case ArithOp.Div =>
                      x.divide(y, DivContext)
                    case ArithOp.IntDiv =>
                      x.divideToIntegralValue(y)
                    case _ =>
                      x.remainder(y)
          if z == null then
            if resultNulls eq nulls then
              resultNulls =
                if nulls == null then
                  new Array[Boolean](n)
                else
                  nulls.clone()
            resultNulls(i) = true
          else
            o(i) = ValueOps.rescale(z, out.scale)
        end if
        i += 1
      end while
      // Try to narrow back to Long storage
      val bld = VecBuilder(out, n)
      i = 0
      while i < n do
        if resultNulls != null && resultNulls(i) then
          bld.addNull()
        else
          bld.add(o(i))
        i += 1
      bld.result()
    end if

  end decimalArith

  /** The value of a row-independent expression, evaluated once */
  def scalarOf(e: VExpr): Option[Any] =
    if e.isConstant then
      Some(e.eval(Batch.ofRows(1)).get(0))
    else
      None

  /** Fill `o(i) = test(i)` for every row, with the comparison operator resolved outside the loop */
  private inline def cmpLoop(op: CmpOp, n: Int, o: Array[Boolean], inline c: Int => Int): Unit =
    var i = 0
    op match
      case CmpOp.Eq =>
        while i < n do
          o(i) = c(i) == 0
          i += 1
      case CmpOp.Ne =>
        while i < n do
          o(i) = c(i) != 0
          i += 1
      case CmpOp.Lt =>
        while i < n do
          o(i) = c(i) < 0
          i += 1
      case CmpOp.Le =>
        while i < n do
          o(i) = c(i) <= 0
          i += 1
      case CmpOp.Gt =>
        while i < n do
          o(i) = c(i) > 0
          i += 1
      case CmpOp.Ge =>
        while i < n do
          o(i) = c(i) >= 0
          i += 1

  /** Compare a vector with a scalar of type `vt` (the vector's type) */
  def compareScalar(op: CmpOp, l: Vec, v: Any, vt: PType): Vec =
    val n = l.size
    if v == null then
      return BoolVec(new Array[Boolean](n), Vec.allTrue(n))
    val o = new Array[Boolean](n)
    l match
      case a: IntVec if vt == a.ptype =>
        val x = a.values
        val y = ValueOps.toInt(v)
        cmpLoop(op, n, o, i => Integer.compare(x(i), y))
        BoolVec(o, a.nulls)
      case a: LongVec if vt == a.ptype && !a.ptype.isDecimal =>
        val x = a.values
        val y = ValueOps.toLong(v)
        cmpLoop(op, n, o, i => java.lang.Long.compare(x(i), y))
        BoolVec(o, a.nulls)
      case a: LongVec if vt == a.ptype =>
        unscaledOf(v, a.ptype) match
          case Some(y) =>
            val x = a.values
            cmpLoop(op, n, o, i => java.lang.Long.compare(x(i), y))
            BoolVec(o, a.nulls)
          case None =>
            compare(op, l, Vec.constant(vt, v, n))
      case a: DoubleVec if vt == PType.Float64 =>
        val x = a.values
        val y = ValueOps.toDouble(v)
        cmpLoop(op, n, o, i => java.lang.Double.compare(x(i), y))
        BoolVec(o, a.nulls)
      case a: StrVec if vt == PType.Str =>
        val x     = a.values
        val y     = v.toString
        val nulls = a.nulls
        if nulls == null then
          cmpLoop(op, n, o, i => x(i).compareTo(y))
        else
          // NULL slots may hold null strings
          cmpLoop(
            op,
            n,
            o,
            i =>
              if nulls(i) then
                0
              else
                x(i).compareTo(y)
          )
        BoolVec(o, nulls)
      case _ =>
        compare(op, l, Vec.constant(vt, v, n))
    end match

  end compareScalar

  /** The unscaled Long of a decimal value at the scale of `t`, when it is exact and fits */
  private def unscaledOf(v: Any, t: PType): Option[Long] =
    t match
      case PType.Decimal(_, s) =>
        try
          val d = ValueOps.toBigDecimal(v).setScale(s)
          val u = d.unscaledValue()
          if u.bitLength() < 64 then
            Some(u.longValue())
          else
            None
        catch
          case _: ArithmeticException =>
            None
      case _ =>
        None

  def compare(op: CmpOp, l: Vec, r: Vec): Vec =
    val n     = l.size
    val nulls = orNulls(l.nulls, r.nulls, n)
    val o     = new Array[Boolean](n)
    (l, r) match
      case (a: IntVec, b: IntVec) =>
        val x = a.values
        val y = b.values
        cmpLoop(op, n, o, i => Integer.compare(x(i), y(i)))
      case (a: LongVec, b: LongVec) if a.ptype == b.ptype =>
        val x = a.values
        val y = b.values
        cmpLoop(op, n, o, i => java.lang.Long.compare(x(i), y(i)))
      case (a: DoubleVec, b: DoubleVec) =>
        val x = a.values
        val y = b.values
        cmpLoop(op, n, o, i => java.lang.Double.compare(x(i), y(i)))
      case (a: StrVec, b: StrVec) =>
        val x = a.values
        val y = b.values
        var i = 0
        while i < n do
          if nulls == null || !nulls(i) then
            o(i) = op.test(x(i).compareTo(y(i)))
          i += 1
      case (a: BoolVec, b: BoolVec) =>
        var i = 0
        while i < n do
          o(i) = op.test(java.lang.Boolean.compare(a.values(i), b.values(i)))
          i += 1
      case _ =>
        var i = 0
        while i < n do
          if nulls == null || !nulls(i) then
            val x = l.get(i)
            val y = r.get(i)
            if x != null && y != null then
              o(i) = op.test(ValueOps.compare(x, y))
          i += 1
    end match
    BoolVec(o, nulls)

  end compare

  /** Three-valued AND */
  def and(l: Vec, r: Vec): Vec = logical(asBool(l), asBool(r), isAnd = true)

  /** Three-valued OR */
  def or(l: Vec, r: Vec): Vec = logical(asBool(l), asBool(r), isAnd = false)

  /**
    * Three-valued AND / OR over raw arrays. The value slot of a NULL row is unspecified, so values
    * are masked by their null arrays. A result row is NULL only when no known operand decides it (a
    * known false for AND, a known true for OR) and an operand is NULL
    */
  private def logical(a: BoolVec, b: BoolVec, isAnd: Boolean): BoolVec =
    val n  = a.size
    val av = a.values
    val bv = b.values
    val an = a.nulls
    val bn = b.nulls
    val o  = new Array[Boolean](n)
    var i  = 0
    if an eq bn then
      // No nulls on either side, or one shared null array (e.g., BETWEEN on one column): a row is
      // NULL exactly when both operands are, so the null array is reused as is
      if isAnd then
        while i < n do
          o(i) = av(i) & bv(i)
          i += 1
      else
        while i < n do
          o(i) = av(i) | bv(i)
          i += 1
      BoolVec(o, an)
    else
      val nulls = new Array[Boolean](n)
      var any   = false
      if an == null || bn == null then
        // x: the operand without nulls, y (with nulls ny): the other
        val (xv, yv, ny) =
          if an == null then
            (av, bv, bn)
          else
            (bv, av, an)
        if isAnd then
          while i < n do
            val yNull = ny(i)
            val x     = xv(i)
            o(i) = x & yv(i) & !yNull
            val isNull = x & yNull
            nulls(i) = isNull
            any = any | isNull
            i += 1
        else
          while i < n do
            val yNull = ny(i)
            val x     = xv(i)
            o(i) = x | (yv(i) & !yNull)
            val isNull = !x & yNull
            nulls(i) = isNull
            any = any | isNull
            i += 1
      else if isAnd then
        while i < n do
          val aN     = an(i)
          val bN     = bn(i)
          val aFalse = !av(i) & !aN
          val bFalse = !bv(i) & !bN
          o(i) = av(i) & !aN & bv(i) & !bN
          val isNull = !(aFalse | bFalse) & (aN | bN)
          nulls(i) = isNull
          any = any | isNull
          i += 1
      else
        while i < n do
          val aN = an(i)
          val bN = bn(i)
          val t  = (av(i) & !aN) | (bv(i) & !bN)
          o(i) = t
          val isNull = !t & (aN | bN)
          nulls(i) = isNull
          any = any | isNull
          i += 1
      end if
      BoolVec(
        o,
        if any then
          nulls
        else
          null
      )
    end if

  end logical

  /** Rows where a boolean vector is true (NULL counts as false) */
  def trueMask(v: Vec): Array[Boolean] =
    val b = asBool(v)
    if b.nulls == null then
      b.values
    else
      val o = new Array[Boolean](b.size)
      var i = 0
      while i < o.length do
        o(i) = b.values(i) && !b.nulls(i)
        i += 1
      o

end Kernels

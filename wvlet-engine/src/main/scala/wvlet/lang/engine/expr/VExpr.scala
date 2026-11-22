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

/** A reference to an input column */
case class ColRef(index: Int, ptype: PType, name: String) extends VExpr:
  def eval(b: Batch): Vec          = b.columns(index)
  def sig: String                  = s"#${index}"
  override def isConstant: Boolean = false

case class Const(value: Any, ptype: PType) extends VExpr:
  def eval(b: Batch): Vec          = Vec.constant(ptype, value, b.size)
  def sig: String                  = s"lit(${value}:${ptype})"
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

/** Comparison of two operands already cast to the same type */
case class Compare(op: CmpOp, left: VExpr, right: VExpr) extends VExpr:
  override def children: Seq[VExpr] = Seq(left, right)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"(${left.sig}${op.symbol}${right.sig})"
  def eval(b: Batch): Vec           = Kernels.compare(op, left.eval(b), right.eval(b))

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

/** `x IN (constants...)`, with SQL NULL semantics */
case class InList(child: VExpr, values: Set[Any], hasNullValue: Boolean, negate: Boolean)
    extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"in(${child.sig},${values.mkString(",")},${negate})"
  def eval(b: Batch): Vec           =
    val v                     = child.eval(b)
    val o                     = new Array[Boolean](b.size)
    var nulls: Array[Boolean] = null
    var i                     = 0
    while i < b.size do
      val x = v.get(i)
      if x == null then
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
    val n       = b.size
    val chosen  = Array.fill(n)(-1)
    var pending = n
    val conds   = whens.map(_._1)
    var k       = 0
    while k < conds.size && pending > 0 do
      val c = Kernels.asBool(conds(k).eval(b))
      var i = 0
      while i < n do
        if chosen(i) < 0 && c.values(i) && !c.isNull(i) then
          chosen(i) = k
          pending -= 1
        i += 1
      k += 1
    val results = whens.map(w => Kernels.castTo(w._2.eval(b), ptype))
    val elseVec = elseExpr.map(e => Kernels.castTo(e.eval(b), ptype))
    val out     = VecBuilder(ptype, n)
    var i       = 0
    while i < n do
      val c = chosen(i)
      if c >= 0 then
        out.add(results(c).get(i))
      else
        elseVec match
          case Some(e) =>
            out.add(e.get(i))
          case None =>
            out.addNull()
      i += 1
    out.result()

  end eval

end CaseWhen

case class Coalesce(args: Seq[VExpr], ptype: PType) extends VExpr:
  override def children: Seq[VExpr] = args
  def sig: String                   = s"coalesce(${args.map(_.sig).mkString(",")})"
  def eval(b: Batch): Vec           =
    val vs  = args.map(a => Kernels.castTo(a.eval(b), ptype))
    val out = VecBuilder(ptype, b.size)
    var i   = 0
    while i < b.size do
      var j     = 0
      var found = false
      while !found && j < vs.size do
        val v = vs(j).get(i)
        if v != null then
          out.add(v)
          found = true
        j += 1
      if !found then
        out.addNull()
      i += 1
    out.result()

case class LikeExpr(
    child: VExpr,
    pattern: String,
    negate: Boolean,
    caseInsensitive: Boolean = false,
    escape: Option[Char] = None
) extends VExpr:
  override def children: Seq[VExpr] = Seq(child)
  def ptype: PType                  = PType.Bool
  def sig: String                   = s"like(${child.sig},${pattern},${negate},${caseInsensitive})"
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
    if a == null then
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
      case other =>
        val o = new Array[Double](other.size)
        var k = 0
        while k < o.length do
          val x = other.get(k)
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
      case (i: IntVec, PType.Int64) if i.ptype == PType.Int32 =>
        LongVec(PType.Int64, asLongs(i), i.nulls)
      case (_: IntVec | _: LongVec, PType.Float64)
          if !v.ptype.isDecimal && v.ptype != PType.Date && v.ptype != PType.Timestamp =>
        DoubleVec(asDoubles(v), v.nulls)
      case (_, d: PType.Decimal) if v.ptype.isIntegral || v.ptype.isDecimal =>
        val u = asUnscaled(v, d.scale)
        if u != null then
          LongVec(d, u, v.nulls)
        else
          genericCast(v, to, tryCast)
      case _ =>
        genericCast(v, to, tryCast)

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
        IntVec(PType.Int32, i.values.map(x => -x), i.nulls)
      case l: LongVec if l.ptype != PType.Timestamp =>
        LongVec(l.ptype, l.values.map(x => -x), l.nulls)
      case d: DoubleVec =>
        DoubleVec(d.values.map(x => -x), d.nulls)
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
                else if op == ArithOp.Mod then
                  o(i) = a(i) % b(i)
                else
                  o(i) = a(i) / b(i)
              i += 1
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
            while i < n do
              if b(i) == 0.0 then
                if resultNulls eq nulls then
                  resultNulls =
                    if nulls == null then
                      new Array[Boolean](n)
                    else
                      nulls.clone()
                resultNulls(i) = true
              else
                o(i) =
                  op match
                    case ArithOp.Div =>
                      a(i) / b(i)
                    case ArithOp.IntDiv =>
                      math.floor(a(i) / b(i))
                    case _ =>
                      a(i) % b(i)
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
    val n                    = l.size
    def scaleOf(v: Vec): Int =
      v.ptype match
        case PType.Decimal(_, s) =>
          s
        case _ =>
          0
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
            while ok && i < n do
              val x = a(i)
              val y = b(i)
              val z =
                if op == ArithOp.Add then
                  x + y
                else
                  x - y
              // Overflow iff both operands have the same sign and the result's sign differs
              if op == ArithOp.Add && ((x ^ z) & (y ^ z)) < 0 then
                ok = false
              else if op == ArithOp.Sub && ((x ^ y) & (x ^ z)) < 0 then
                ok = false
              o(i) = z
              i += 1
            if ok then
              LongVec(out, o, nulls)
            else
              null
        case ArithOp.Mul if scaleOf(l) + scaleOf(r) == out.scale =>
          val a = asUnscaled(l, scaleOf(l))
          val b = asUnscaled(r, scaleOf(r))
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

  def compare(op: CmpOp, l: Vec, r: Vec): Vec =
    val n     = l.size
    val nulls = orNulls(l.nulls, r.nulls, n)
    val o     = new Array[Boolean](n)
    (l, r) match
      case (a: IntVec, b: IntVec) =>
        val x = a.values
        val y = b.values
        var i = 0
        while i < n do
          o(i) = op.test(Integer.compare(x(i), y(i)))
          i += 1
      case (a: LongVec, b: LongVec) if a.ptype == b.ptype =>
        val x = a.values
        val y = b.values
        var i = 0
        while i < n do
          o(i) = op.test(java.lang.Long.compare(x(i), y(i)))
          i += 1
      case (a: DoubleVec, b: DoubleVec) =>
        val x = a.values
        val y = b.values
        var i = 0
        while i < n do
          o(i) = op.test(java.lang.Double.compare(x(i), y(i)))
          i += 1
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
  def and(l: Vec, r: Vec): Vec =
    val a = asBool(l)
    val b = asBool(r)
    val n = a.size
    val o = new Array[Boolean](n)
    if a.nulls == null && b.nulls == null then
      var i = 0
      while i < n do
        o(i) = a.values(i) && b.values(i)
        i += 1
      BoolVec(o, null)
    else
      val nulls = new Array[Boolean](n)
      var any   = false
      var i     = 0
      while i < n do
        val an = a.isNull(i)
        val bn = b.isNull(i)
        val av = a.values(i)
        val bv = b.values(i)
        if (!an && !av) || (!bn && !bv) then
          o(i) = false
        else if an || bn then
          nulls(i) = true
          any = true
        else
          o(i) = true
        i += 1
      BoolVec(
        o,
        if any then
          nulls
        else
          null
      )
    end if

  end and

  /** Three-valued OR */
  def or(l: Vec, r: Vec): Vec =
    val a = asBool(l)
    val b = asBool(r)
    val n = a.size
    val o = new Array[Boolean](n)
    if a.nulls == null && b.nulls == null then
      var i = 0
      while i < n do
        o(i) = a.values(i) || b.values(i)
        i += 1
      BoolVec(o, null)
    else
      val nulls = new Array[Boolean](n)
      var any   = false
      var i     = 0
      while i < n do
        val an = a.isNull(i)
        val bn = b.isNull(i)
        val av = a.values(i)
        val bv = b.values(i)
        if (!an && av) || (!bn && bv) then
          o(i) = true
        else if an || bn then
          nulls(i) = true
          any = true
        i += 1
      BoolVec(
        o,
        if any then
          nulls
        else
          null
      )

  end or

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

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
package wvlet.lang.engine.gpu

import ai.rapids.cudf.BinaryOp
import ai.rapids.cudf.BinaryOperable
import ai.rapids.cudf.ColumnVector
import ai.rapids.cudf.DType
import ai.rapids.cudf.Scalar
import wvlet.lang.api.StatusCode
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.PType

import scala.util.Try

/**
  * A value of an expression on the device: a column of the input's rows, or a scalar for constants.
  * Closing releases what the operand owns (input columns are borrowed).
  */
private[gpu] sealed trait Operand extends AutoCloseable:
  def operable: BinaryOperable

  /** An owned column of `rows` values */
  def toColumn(rows: Int): ColumnVector

private[gpu] final class ColOperand(val cv: ColumnVector, owned: Boolean) extends Operand:
  def operable: BinaryOperable          = cv
  def toColumn(rows: Int): ColumnVector = cv.incRefCount()
  override def close(): Unit            =
    if owned then
      cv.close()

private[gpu] final class LitOperand(val s: Scalar) extends Operand:
  def operable: BinaryOperable          = s
  def toColumn(rows: Int): ColumnVector = ColumnVector.fromScalar(s, rows)
  override def close(): Unit            = s.close()

/**
  * Evaluates engine expressions ([[VExpr]] over input positions) with cudf. [[supports]] decides at
  * plan time whether an expression can run on the GPU with the CPU engine's semantics; [[eval]]
  * then computes it over a table of input columns.
  */
object GpuExprs:
  import GpuTypes.*

  // ---- plan-time support ----

  def supports(e: VExpr): Boolean =
    e match
      case ColRef(_, p, _) =>
        dtypeOf(p).isDefined
      case Const(v, p) =>
        // Checked on the host: planning never allocates on the device
        dtypeOf(p).isDefined && (v == null || convertible(v, p))
      case a: Arith =>
        supports(a.left) && supports(a.right) && arithSupported(a)
      case Negate(c) =>
        supports(c) && (c.ptype.isNumeric)
      case c: Compare =>
        supports(c.left) && supports(c.right) && comparable(c.left.ptype, c.right.ptype).isDefined
      case AndExpr(l, r) =>
        isBool(l) && isBool(r) && supports(l) && supports(r)
      case OrExpr(l, r) =>
        isBool(l) && isBool(r) && supports(l) && supports(r)
      case NotExpr(c) =>
        isBool(c) && supports(c)
      case IsNullExpr(c, _) =>
        supports(c)
      case InList(c, values, _, _) =>
        supports(c) && inListType(c.ptype) && values.forall(v => v != null)
      case l: LikeExpr =>
        !l.caseInsensitive && l.escape.isEmpty && l.child.ptype == PType.Str && supports(l.child)
      case cw: CaseWhen =>
        dtypeOf(cw.ptype).isDefined &&
        cw.whens
          .forall((c, v) =>
            isBool(c) && supports(c) && supports(v) && castable(v.ptype, cw.ptype)
          ) && cw.elseExpr.forall(v => supports(v) && castable(v.ptype, cw.ptype))
      case c: CastExpr =>
        !c.tryCast && supports(c.child) && castable(c.child.ptype, c.ptype)
      case f: RowFn =>
        rowFn(f).isDefined
      case _ =>
        false

  private def isBool(e: VExpr): Boolean = e.ptype == PType.Bool || e.ptype == PType.Null

  private def inListType(p: PType): Boolean =
    p == PType.Str || p == PType.Int32 || p == PType.Int64 || p == PType.Date

  private def scaleOf(p: PType): Option[Int] =
    p match
      case PType.Decimal(_, s) =>
        Some(s)
      case PType.Int32 | PType.Int64 =>
        Some(0)
      case _ =>
        None

  private def arithSupported(a: Arith): Boolean =
    a.ptype match
      case PType.Decimal(_, s) =>
        (scaleOf(a.left.ptype), scaleOf(a.right.ptype)) match
          case (Some(l), Some(r)) =>
            a.op match
              case ArithOp.Add | ArithOp.Sub =>
                math.max(l, r) == s
              case ArithOp.Mul =>
                l + r == s
              case _ =>
                false
          case _ =>
            false
      case PType.Float64 =>
        a.left.ptype.isNumeric && a.right.ptype.isNumeric && a.op != ArithOp.Mod
      case PType.Int32 | PType.Int64 =>
        // + - * overflow raises an error on the CPU (Math.addExact) but wraps in cudf: offload them
        // only over constants whose exact result fits (e.g. `1 + 10` in TPC-H q19)
        a.left.ptype.isIntegral && a.right.ptype.isIntegral && {
          a.op match
            case ArithOp.Div | ArithOp.IntDiv | ArithOp.Mod =>
              true
            case op =>
              (a.left, a.right) match
                case (Const(x: Number, _), Const(y: Number, _)) =>
                  Try {
                    val (l, r) = (x.longValue(), y.longValue())
                    val z      =
                      op match
                        case ArithOp.Add =>
                          Math.addExact(l, r)
                        case ArithOp.Sub =>
                          Math.subtractExact(l, r)
                        case _ =>
                          Math.multiplyExact(l, r)
                    a.ptype == PType.Int64 || z.isValidInt
                  }.getOrElse(false)
                case _ =>
                  false
        }
      case _ =>
        false

  /**
    * The type both sides of a comparison are cast to on the device, or None when they aren't
    * comparable there
    */
  private def comparable(l: PType, r: PType): Option[PType] =
    (l, r) match
      case _ if l == r && dtypeOf(l).isDefined =>
        Some(l)
      case (PType.Int32 | PType.Int64, PType.Int32 | PType.Int64) =>
        Some(PType.Int64)
      case (PType.Float64, _) if r.isNumeric =>
        Some(PType.Float64)
      case (_, PType.Float64) if l.isNumeric =>
        Some(PType.Float64)
      case _ if (l.isDecimal || l.isIntegral) && (r.isDecimal || r.isIntegral) =>
        val ls = scaleOf(l).get
        val rs = scaleOf(r).get
        val s  = math.max(ls, rs)
        // Digits before the point, plus the larger scale
        val intDigits = math.max(precisionOf(l) - ls, precisionOf(r) - rs)
        // Wider than 128 bits: casting both sides would overflow, so compare on the CPU
        Option.when(intDigits + s <= 38)(PType.decimal(intDigits + s, s))
      case _ =>
        None

  private def precisionOf(p: PType): Int =
    p match
      case PType.Decimal(pr, _) =>
        pr
      case PType.Int32 =>
        10
      case PType.Int64 =>
        19
      case _ =>
        38

  /** Casts that give the CPU engine's result on the device */
  private def castable(from: PType, to: PType): Boolean =
    (from, to) match
      case _ if from == to =>
        true
      case (PType.Null, _) =>
        dtypeOf(to).isDefined
      case (PType.Int32 | PType.Int64, PType.Int32 | PType.Int64 | PType.Float64) =>
        true
      case (PType.Int32 | PType.Int64, d: PType.Decimal) =>
        d.precision - d.scale >= precisionOf(from).min(19)
      case (f: PType.Decimal, t: PType.Decimal) =>
        // Only exact rescaling: rounding to fewer decimal places differs from the CPU engine
        t.scale >= f.scale && t.precision - t.scale >= f.precision - f.scale
      case (_: PType.Decimal, PType.Float64) =>
        true
      case (PType.Float64, PType.Float64) =>
        true
      case _ =>
        false

  /** The cudf form of a row function the GPU supports */
  private enum GpuFn:
    case DatePart(component: String)
    case Substring(start: Int, length: Option[Int])

  private def rowFn(f: RowFn): Option[GpuFn] =
    f.name match
      case "date_part"
          if f.args.size == 1 && f.args.head.ptype == PType.Date && supports(f.args.head) =>
        // The part name is bound into the function: identify it from its value on 2001-02-03
        val day = java.time.LocalDate.of(2001, 2, 3).toEpochDay.toInt
        Try(f.f(Array[Any](day)))
          .toOption
          .collect {
            case 2001L =>
              GpuFn.DatePart("year")
            case 2L =>
              GpuFn.DatePart("month")
            case 3L =>
              GpuFn.DatePart("day")
          }
      case "substring" if f.args.head.ptype == PType.Str && supports(f.args.head) =>
        f.args.tail match
          case Seq(Const(s: Long, _)) if s >= 1 && s < Int.MaxValue =>
            Some(GpuFn.Substring(s.toInt, None))
          case Seq(Const(s: Long, _), Const(n: Long, _))
              if s >= 1 && n >= 0 && s + n < Int.MaxValue =>
            Some(GpuFn.Substring(s.toInt, Some(n.toInt)))
          case _ =>
            None
      case _ =>
        None

  // ---- evaluation ----

  /** Evaluate `e` over `input` (columns by position, `rows` rows each) */
  def eval(e: VExpr, input: IndexedSeq[ColumnVector], rows: Int): Operand =
    e match
      case ColRef(i, _, _) =>
        ColOperand(input(i), owned = false)
      case Const(v, p) =>
        LitOperand(scalarOf(v, p).get)
      case a: Arith =>
        arith(a, input, rows)
      case Negate(c) =>
        val d = dtype(c.ptype)
        withOperand(eval(c, input, rows)) { o =>
          val zero =
            if d.isDecimalType then
              Scalar.fromDecimal(java.math.BigInteger.ZERO, d)
            else
              scalarOf(0, c.ptype).get
          try col(zero.binaryOp(BinaryOp.SUB, o.operable, d))
          finally zero.close()
        }
      case c: Compare =>
        val t = comparable(c.left.ptype, c.right.ptype).get
        withOperand(evalAs(c.left, t, input, rows)) { l =>
          withOperand(evalAs(c.right, t, input, rows)) { r =>
            val op =
              c.op match
                case CmpOp.Eq =>
                  BinaryOp.EQUAL
                case CmpOp.Ne =>
                  BinaryOp.NOT_EQUAL
                case CmpOp.Lt =>
                  BinaryOp.LESS
                case CmpOp.Le =>
                  BinaryOp.LESS_EQUAL
                case CmpOp.Gt =>
                  BinaryOp.GREATER
                case CmpOp.Ge =>
                  BinaryOp.GREATER_EQUAL
            binary(l, r, op, DType.BOOL8, rows)
          }
        }
      case AndExpr(l, r) =>
        logical(l, r, BinaryOp.NULL_LOGICAL_AND, input, rows)
      case OrExpr(l, r) =>
        logical(l, r, BinaryOp.NULL_LOGICAL_OR, input, rows)
      case NotExpr(c) =>
        withColumn(evalAs(c, PType.Bool, input, rows), rows)(cv => col(cv.not()))
      case IsNullExpr(c, negate) =>
        withColumn(eval(c, input, rows), rows) { cv =>
          col(
            if negate then
              cv.isNotNull()
            else
              cv.isNull()
          )
        }
      case InList(c, values, hasNull, negate) =>
        inList(c, values, hasNull, negate, input, rows)
      case l: LikeExpr =>
        withColumn(eval(l.child, input, rows), rows) { cv =>
          val pattern = Scalar.fromString(l.pattern)
          val escape  = Scalar.fromString("")
          try
            val m = cv.like(pattern, escape)
            if l.negate then
              try col(m.not())
              finally m.close()
            else
              col(m)
          finally
            pattern.close()
            escape.close()
        }
      case cw: CaseWhen =>
        caseWhen(cw, input, rows)
      case c: CastExpr =>
        evalAs(c.child, c.ptype, input, rows)
      case f: RowFn =>
        rowFn(f).get match
          case GpuFn.DatePart(part) =>
            withColumn(eval(f.args.head, input, rows), rows) { cv =>
              val p =
                part match
                  case "year" =>
                    cv.year()
                  case "month" =>
                    cv.month()
                  case _ =>
                    cv.day()
              try col(p.castTo(DType.INT64))
              finally p.close()
            }
          case GpuFn.Substring(start, length) =>
            withColumn(eval(f.args.head, input, rows), rows) { cv =>
              col(
                length match
                  case Some(n) =>
                    cv.substring(start - 1, start - 1 + n)
                  case None =>
                    cv.substring(start - 1)
              )
            }
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported expression on the GPU: ${other.sig}")

  /** Evaluate `e` as a column of `rows` values of type `p` */
  def evalColumn(e: VExpr, p: PType, input: IndexedSeq[ColumnVector], rows: Int): ColumnVector =
    val o = evalAs(e, p, input, rows)
    try o.toColumn(rows)
    finally o.close()

  /** Evaluate `e`, cast to `p` when its type differs */
  def evalAs(e: VExpr, p: PType, input: IndexedSeq[ColumnVector], rows: Int): Operand =
    e match
      case Const(v, _) if e.ptype != p =>
        // Convert constants on the host, exactly
        val converted: Any =
          (v, p) match
            case (null, _) =>
              null
            case (n: Number, d: PType.Decimal) =>
              java.math.BigDecimal(n.toString)
            case (n: Number, PType.Float64) =>
              n.doubleValue()
            case (n: Number, PType.Int64) =>
              n.longValue()
            case (n: Number, PType.Int32) =>
              n.intValue()
            case (other, _) =>
              other
        scalarOf(converted, p) match
          case Some(s) =>
            LitOperand(s)
          case None =>
            throw StatusCode
              .INVALID_ARGUMENT
              .newException(s"Cannot convert ${v} to ${p} on the GPU")
      case _ =>
        val o = eval(e, input, rows)
        val d = dtype(p)
        o match
          case c: ColOperand if c.cv.getType != d =>
            try col(c.cv.castTo(d))
            finally c.close()
          case s: LitOperand if s.s.getType != d =>
            // A scalar computed on the device (rare): cast it as a column
            try
              val cv = s.toColumn(rows)
              try col(cv.castTo(d))
              finally cv.close()
            finally
              s.close()
          case _ =>
            o

  private def col(cv: ColumnVector): Operand = ColOperand(cv, owned = true)

  private def withOperand[A](o: Operand)(f: Operand => A): A =
    try f(o)
    finally o.close()

  private def withColumn[A](o: Operand, rows: Int)(f: ColumnVector => A): A =
    try
      o match
        case c: ColOperand =>
          f(c.cv)
        case s: LitOperand =>
          val cv = s.toColumn(rows)
          try f(cv)
          finally cv.close()
    finally o.close()

  /** `l op r`, materializing one side when both are scalars */
  private def binary(l: Operand, r: Operand, op: BinaryOp, out: DType, rows: Int): Operand =
    (l, r) match
      case (ls: LitOperand, rs: LitOperand) =>
        val lc = ls.toColumn(rows)
        try col(lc.binaryOp(op, rs.s, out))
        finally lc.close()
      case _ =>
        col(l.operable.binaryOp(op, r.operable, out))

  private def logical(
      l: VExpr,
      r: VExpr,
      op: BinaryOp,
      input: IndexedSeq[ColumnVector],
      rows: Int
  ): Operand =
    withOperand(evalAs(l, PType.Bool, input, rows)) { lo =>
      withOperand(evalAs(r, PType.Bool, input, rows)) { ro =>
        binary(lo, ro, op, DType.BOOL8, rows)
      }
    }

  private def arith(a: Arith, input: IndexedSeq[ColumnVector], rows: Int): Operand =
    val out = dtype(a.ptype)
    a.ptype match
      case d: PType.Decimal =>
        // Each side keeps its own scale, in the output's width (DECIMAL64 or DECIMAL128)
        def side(e: VExpr): Operand =
          val s = scaleOf(e.ptype).get
          evalAs(
            e,
            PType.decimal(
              if out.getTypeId == DType.DTypeEnum.DECIMAL64 then
                18
              else
                38
              ,
              s
            ),
            input,
            rows
          )
        withOperand(side(a.left)) { l =>
          withOperand(side(a.right)) { r =>
            val op =
              a.op match
                case ArithOp.Add =>
                  BinaryOp.ADD
                case ArithOp.Sub =>
                  BinaryOp.SUB
                case _ =>
                  BinaryOp.MUL
            binary(l, r, op, out, rows)
          }
        }
      case _ =>
        withOperand(evalAs(a.left, a.ptype, input, rows)) { l =>
          withOperand(evalAs(a.right, a.ptype, input, rows)) { r =>
            val floating = a.ptype == PType.Float64
            a.op match
              case ArithOp.Add =>
                binary(l, r, BinaryOp.ADD, out, rows)
              case ArithOp.Sub =>
                binary(l, r, BinaryOp.SUB, out, rows)
              case ArithOp.Mul =>
                binary(l, r, BinaryOp.MUL, out, rows)
              case op =>
                val bop =
                  op match
                    case ArithOp.Div if floating =>
                      BinaryOp.TRUE_DIV
                    case ArithOp.IntDiv if floating =>
                      BinaryOp.FLOOR_DIV
                    case ArithOp.Mod =>
                      BinaryOp.MOD
                    case _ =>
                      BinaryOp.DIV
                // x / 0 is NULL, as on the CPU
                withOperand(binary(l, r, bop, out, rows)) { q =>
                  val zero  = LitOperand(scalarOf(0, a.ptype).get)
                  val equal =
                    withOperand(zero)(z => binary(r, z, BinaryOp.EQUAL, DType.BOOL8, rows))
                  withColumn(equal, rows) { isZero =>
                    val nul = Scalar.fromNull(out)
                    try
                      val qc = q.toColumn(rows)
                      try col(isZero.ifElse(nul, qc))
                      finally qc.close()
                    finally
                      nul.close()
                  }
                }
            end match
          }
        }
    end match

  end arith

  private def inList(
      c: VExpr,
      values: Set[Any],
      hasNull: Boolean,
      negate: Boolean,
      input: IndexedSeq[ColumnVector],
      rows: Int
  ): Operand =
    val vs                   = values.toSeq
    val search: ColumnVector =
      c.ptype match
        case PType.Str =>
          ColumnVector.fromStrings(vs.map(_.asInstanceOf[String])*)
        case PType.Int32 =>
          ColumnVector.fromInts(vs.map(_.asInstanceOf[Number].intValue())*)
        case PType.Int64 =>
          ColumnVector.fromLongs(vs.map(_.asInstanceOf[Number].longValue())*)
        case _ =>
          // Date: days since the epoch
          val days = ColumnVector.fromInts(vs.map(_.asInstanceOf[Number].intValue())*)
          try days.castTo(DType.TIMESTAMP_DAYS)
          finally days.close()
    try
      withColumn(eval(c, input, rows), rows) { cv =>
        val found = cv.contains(search)
        val t     = Scalar.fromBool(true)
        val nb    = Scalar.fromNull(DType.BOOL8)
        try
          // Not found in a list with NULL is NULL; a NULL input is NULL
          val r1 =
            if hasNull then
              found.ifElse(t, nb)
            else
              found.incRefCount()
          val isNull = cv.isNull()
          try
            val r2 = isNull.ifElse(nb, r1)
            if negate then
              try col(r2.not())
              finally r2.close()
            else
              col(r2)
          finally
            r1.close()
            isNull.close()
        finally
          found.close()
          t.close()
          nb.close()
      }
    finally search.close()

  end inList

  private def caseWhen(cw: CaseWhen, input: IndexedSeq[ColumnVector], rows: Int): Operand =
    // From the last branch: each WHEN chooses between its value and the branches after it
    var acc: ColumnVector =
      cw.elseExpr match
        case Some(e) =>
          evalColumn(e, cw.ptype, input, rows)
        case None =>
          val n = Scalar.fromNull(dtype(cw.ptype))
          try ColumnVector.fromScalar(n, rows)
          finally n.close()
    try
      cw.whens
        .reverse
        .foreach { (cond, value) =>
          val c = evalColumn(cond, PType.Bool, input, rows)
          try
            val f = Scalar.fromBool(false)
            val m =
              try c.replaceNulls(f)
              finally f.close()
            try
              val v = evalColumn(value, cw.ptype, input, rows)
              try
                val next = m.ifElse(v, acc)
                acc.close()
                acc = next
              finally
                v.close()
            finally
              m.close()
          finally
            c.close()
        }
      val out = acc
      acc = null
      col(out)
    finally
      if acc != null then
        acc.close()

  end caseWhen

end GpuExprs

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

import wvlet.lang.engine.vector.PType
import wvlet.lang.model.plan.RendezvousSampling

/**
  * DuckDB's hash functions (src/common/types/hash.cpp), as DuckDB's `hash(...)` computes them for
  * strings, integers, and booleans. Other values hash their text
  */
object DuckDBHash:
  /** DuckDB's MurmurHash64 finalizer of a 64-bit value */
  def murmurHash64(v: Long): Long =
    var x = v
    x ^= x >>> 32
    x *= 0xd6e8feb86659fd93L
    x ^= x >>> 32
    x *= 0xd6e8feb86659fd93L
    x ^= x >>> 32
    x

  /** DuckDB's hash of a byte string */
  def hashBytes(bytes: Array[Byte]): Long =
    var h   = 0xe17a1465L ^ (bytes.length.toLong * 0xc6a4a7935bd1e995L)
    var pos = 0
    while pos < bytes.length do
      var block = 0L
      var k     = 0
      while k < 8 && pos + k < bytes.length do
        block |= (bytes(pos + k) & 0xffL) << (8 * k)
        k += 1
      h ^= block
      h *= 0xd6e8feb86659fd93L
      pos += 8
    murmurHash64(h)

  /** DuckDB's hash of one value */
  def hash(v: Any): Long =
    v match
      case s: String =>
        hashBytes(s.getBytes("UTF-8"))
      case i: Int =>
        murmurHash64(i.toLong)
      case l: Long =>
        murmurHash64(l)
      case b: Boolean =>
        murmurHash64(
          if b then
            1L
          else
            0L
        )
      case other =>
        hashBytes(other.toString.getBytes("UTF-8"))

  /**
    * DuckDB's combination of the hash `a` of the preceding values with the hash `b` of the next
    * (CombineHashScalar, as of DuckDB 2.0)
    */
  def combineHash(a: Long, b: Long): Long =
    var x = a
    x ^= x >>> 32
    x *= 0xd6e8feb86659fd93L
    x ^ b

  /** DuckDB's `hash(v1, v2, ...)` of one or more values */
  def hashAll(vs: Array[Any]): Long =
    var h = hash(vs(0))
    var i = 1
    while i < vs.length do
      h = combineHash(h, hash(vs(i)))
      i += 1
    h

  /**
    * The filter condition of a rendezvous sample over `keys`: true when every key is non-null and
    * the top [[RendezvousSampling.HashBits]] bits of DuckDB's `hash(seed, keys...)` fall under the
    * threshold of `percentage`. A null key gives null, which filters the row out. The function name
    * carries the rate and seed, since passes match expressions by signature
    */
  def rendezvousSample(keys: Seq[VExpr], percentage: Double, seed: Long): VExpr =
    val threshold = RendezvousSampling.threshold(percentage)
    val shift     = 64 - RendezvousSampling.HashBits
    val seedHash  = murmurHash64(seed)
    RowFn(
      s"rendezvous_sample[${percentage}%,seed=${seed}]",
      keys,
      PType.Bool,
      { row =>
        var h = seedHash
        var i = 0
        while i < row.length do
          h = combineHash(h, hash(row(i)))
          i += 1
        (h >>> shift) < threshold
      }
    )

end DuckDBHash

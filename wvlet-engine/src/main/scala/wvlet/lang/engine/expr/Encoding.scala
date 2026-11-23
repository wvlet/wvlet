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

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.StandardCharsets

/**
  * Base64, hex, and SHA digests of a string's UTF-8 bytes. The engine has no binary type, so
  * decoders return strings. Pure Scala, with no platform-specific dependency (Scala.js has no
  * MessageDigest)
  */
object Encoding:
  private val HexDigits = "0123456789abcdef".toCharArray

  /** Lowercase hex of the bytes */
  def hex(bytes: Array[Byte]): String =
    val out = new Array[Char](bytes.length * 2)
    var i   = 0
    while i < bytes.length do
      val v = bytes(i) & 0xff
      out(2 * i) = HexDigits(v >>> 4)
      out(2 * i + 1) = HexDigits(v & 0x0f)
      i += 1
    String(out)

  private def hexValue(c: Char, s: String): Int =
    val v = Character.digit(c, 16)
    if v < 0 then
      throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid hex string: '${s}'")
    v

  /** Bytes of a hex string; either case. An odd length reads a leading 0, as DuckDB's unhex */
  def unhex(s: String): Array[Byte] =
    val odd = s.length % 2
    val out = new Array[Byte]((s.length + odd) / 2)
    var i   = 0
    while i < out.length do
      val hi =
        if i == 0 && odd == 1 then
          0
        else
          hexValue(s.charAt(2 * i - odd), s)
      val lo = hexValue(s.charAt(2 * i + 1 - odd), s)
      out(i) = ((hi << 4) | lo).toByte
      i += 1
    out

  def toBase64(s: String): String = java
    .util
    .Base64
    .getEncoder
    .encodeToString(s.getBytes(StandardCharsets.UTF_8))

  def fromBase64(s: String): Array[Byte] =
    try
      java.util.Base64.getDecoder.decode(s)
    catch
      case e: IllegalArgumentException =>
        throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid base64 string: '${s}'")

  /** Decode bytes as UTF-8, failing on invalid sequences rather than replacing them */
  def utf8(bytes: Array[Byte]): String =
    try
      StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString
    catch
      case e: CharacterCodingException =>
        throw StatusCode.INVALID_ARGUMENT.newException("The decoded bytes are not valid UTF-8")

  /** Pad a message for SHA-1/SHA-256: 0x80, zeros, and the big-endian bit length */
  private def pad(msg: Array[Byte]): Array[Byte] =
    val bitLen = msg.length.toLong * 8
    val padLen = ((msg.length + 8) / 64 + 1) * 64
    val buf    = new Array[Byte](padLen)
    System.arraycopy(msg, 0, buf, 0, msg.length)
    buf(msg.length) = 0x80.toByte
    var i = 0
    while i < 8 do
      buf(padLen - 1 - i) = ((bitLen >>> (8 * i)) & 0xff).toByte
      i += 1
    buf

  private def word(buf: Array[Byte], o: Int): Int =
    ((buf(o) & 0xff) << 24) |
      ((buf(o + 1) & 0xff) << 16) |
      ((buf(o + 2) & 0xff) << 8) |
      (buf(o + 3) & 0xff)

  private def hexWords(h: Array[Int]): String =
    val bytes = new Array[Byte](h.length * 4)
    var i     = 0
    while i < h.length do
      bytes(4 * i) = (h(i) >>> 24).toByte
      bytes(4 * i + 1) = (h(i) >>> 16).toByte
      bytes(4 * i + 2) = (h(i) >>> 8).toByte
      bytes(4 * i + 3) = h(i).toByte
      i += 1
    hex(bytes)

  /** Lowercase hex SHA-1 digest of the string's UTF-8 bytes */
  def sha1(s: String): String =
    val buf   = pad(s.getBytes(StandardCharsets.UTF_8))
    val h     = Array(0x67452301, 0xefcdab89.toInt, 0x98badcfe.toInt, 0x10325476, 0xc3d2e1f0.toInt)
    val w     = new Array[Int](80)
    var chunk = 0
    while chunk < buf.length do
      var t = 0
      while t < 16 do
        w(t) = word(buf, chunk + 4 * t)
        t += 1
      while t < 80 do
        w(t) = Integer.rotateLeft(w(t - 3) ^ w(t - 8) ^ w(t - 14) ^ w(t - 16), 1)
        t += 1
      var a = h(0)
      var b = h(1)
      var c = h(2)
      var d = h(3)
      var e = h(4)
      t = 0
      while t < 80 do
        val (f, k) =
          if t < 20 then
            ((b & c) | (~b & d), 0x5a827999)
          else if t < 40 then
            (b ^ c ^ d, 0x6ed9eba1)
          else if t < 60 then
            ((b & c) | (b & d) | (c & d), 0x8f1bbcdc.toInt)
          else
            (b ^ c ^ d, 0xca62c1d6.toInt)
        val tmp = Integer.rotateLeft(a, 5) + f + e + k + w(t)
        e = d
        d = c
        c = Integer.rotateLeft(b, 30)
        b = a
        a = tmp
        t += 1
      h(0) += a
      h(1) += b
      h(2) += c
      h(3) += d
      h(4) += e
      chunk += 64
    end while
    hexWords(h)

  end sha1

  private val K256 = Array(
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
  ).map(_.toInt)

  /** Lowercase hex SHA-256 digest of the string's UTF-8 bytes */
  def sha256(s: String): String =
    val buf = pad(s.getBytes(StandardCharsets.UTF_8))
    val h   = Array(
      0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
    ).map(_.toInt)
    val w     = new Array[Int](64)
    var chunk = 0
    while chunk < buf.length do
      var t = 0
      while t < 16 do
        w(t) = word(buf, chunk + 4 * t)
        t += 1
      while t < 64 do
        val s0 =
          Integer.rotateRight(w(t - 15), 7) ^ Integer.rotateRight(w(t - 15), 18) ^ (w(t - 15) >>> 3)
        val s1 =
          Integer.rotateRight(w(t - 2), 17) ^ Integer.rotateRight(w(t - 2), 19) ^ (w(t - 2) >>> 10)
        w(t) = w(t - 16) + s0 + w(t - 7) + s1
        t += 1
      var a  = h(0)
      var b  = h(1)
      var c  = h(2)
      var d  = h(3)
      var e  = h(4)
      var f  = h(5)
      var g  = h(6)
      var hh = h(7)
      t = 0
      while t < 64 do
        val s1 = Integer.rotateRight(e, 6) ^ Integer.rotateRight(e, 11) ^ Integer.rotateRight(e, 25)
        val ch = (e & f) ^ (~e & g)
        val t1 = hh + s1 + ch + K256(t) + w(t)
        val s0 = Integer.rotateRight(a, 2) ^ Integer.rotateRight(a, 13) ^ Integer.rotateRight(a, 22)
        val maj = (a & b) ^ (a & c) ^ (b & c)
        val t2  = s0 + maj
        hh = g
        g = f
        f = e
        e = d + t1
        d = c
        c = b
        b = a
        a = t1 + t2
        t += 1
      h(0) += a
      h(1) += b
      h(2) += c
      h(3) += d
      h(4) += e
      h(5) += f
      h(6) += g
      h(7) += hh
      chunk += 64
    end while
    hexWords(h)

  end sha256

end Encoding

package wvlet.lang.server

import java.security.SecureRandom
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicInteger

/**
  * Issues query ids of the form `yyyyMMdd_HHmmss_<seq>_<server>`, following Trino:
  *
  *   - the UTC creation time to the second, so ids sort by time and show when a query ran
  *   - a 5-digit per-server sequence (wrapping at 99999), so queries created close together differ
  *     at a fixed position
  *   - a 5-character server instance id chosen at startup, so ids of different servers, or of one
  *     server across restarts, never collide without a persisted counter
  *
  * ULIDs and UUIDv7s were dropped because ids issued close together share their leading characters
  * and differ only in the trailing random part.
  */
class QueryIdGenerator(
    val serverId: String = QueryIdGenerator.randomServerId(),
    clock: () => Instant = () => Instant.now()
):
  require(QueryIdGenerator.isServerId(serverId), s"Invalid server id: ${serverId}")

  private val counter = AtomicInteger(0)

  /** Issue the next query id */
  def next(): String =
    val seq = Math.floorMod(counter.getAndIncrement(), QueryIdGenerator.MaxSequence)
    s"${QueryIdGenerator.timeFormat.format(clock())}_${f"${seq}%05d"}_${serverId}"

object QueryIdGenerator:
  private val MaxSequence = 100000

  // RFC 4648 base-32 in lower case: no 0/1/8/9, which read like o/l/b/g
  private val ServerIdAlphabet = "abcdefghijklmnopqrstuvwxyz234567"
  private val ServerIdLength   = 5

  private val timeFormat = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneOffset.UTC)

  private val QueryIdPattern = "^\\d{8}_\\d{6}_\\d{5}_[a-z2-7]{5}$".r
  private val random         = SecureRandom()

  /** A random 5-character server instance id */
  def randomServerId(): String =
    val b = StringBuilder()
    for _ <- 0 until ServerIdLength do
      b += ServerIdAlphabet.charAt(random.nextInt(ServerIdAlphabet.length))
    b.result()

  /** True if the string has the shape of a server instance id */
  def isServerId(s: String): Boolean =
    s.length == ServerIdLength && s.forall(c => ServerIdAlphabet.indexOf(c) >= 0)

  /** True if the string has the shape of an issued query id */
  def isQueryId(s: String): Boolean = QueryIdPattern.matches(s)

end QueryIdGenerator

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
package wvlet.lang.runner

import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.ConfigItem

/**
  * Orchestration configuration of a single stage, extracted from its `with { ... }` block
  */
case class StageExecutionConfig(
    retries: Int = 0,
    retryDelayMillis: Long = 1000L,
    backoff: String = StageExecutionConfig.BACKOFF_EXPONENTIAL,
    maxRetryDelayMillis: Option[Long] = None,
    timeoutMillis: Option[Long] = None,
    heartbeatMillis: Option[Long] = None,
    pollIntervalMillis: Long = StageExecutionConfig.DEFAULT_POLL_INTERVAL_MILLIS,
    // Activity stages only: the number of items running at once, and which items a rerun executes
    // again: those with an unknown outcome (retryUncertain) or a failure (retryFailed)
    concurrency: Option[Int] = None,
    retryUncertain: Boolean = false,
    retryFailed: Boolean = false
):
  def withHeartbeatMillis(millis: Long): StageExecutionConfig = copy(heartbeatMillis = Some(millis))

  def withPollIntervalMillis(millis: Long): StageExecutionConfig = copy(pollIntervalMillis = millis)

  def noHeartbeat(): StageExecutionConfig                           = copy(heartbeatMillis = None)
  def withRetries(retries: Int): StageExecutionConfig               = copy(retries = retries)
  def withRetryDelayMillis(delayMillis: Long): StageExecutionConfig = copy(retryDelayMillis =
    delayMillis
  )

  def withBackoff(backoff: String): StageExecutionConfig               = copy(backoff = backoff)
  def withMaxRetryDelayMillis(delayMillis: Long): StageExecutionConfig = copy(maxRetryDelayMillis =
    Some(delayMillis)
  )

  def noMaxRetryDelay(): StageExecutionConfig                  = copy(maxRetryDelayMillis = None)
  def withTimeoutMillis(millis: Long): StageExecutionConfig    = copy(timeoutMillis = Some(millis))
  def noTimeout(): StageExecutionConfig                        = copy(timeoutMillis = None)
  def withConcurrency(n: Int): StageExecutionConfig            = copy(concurrency = Some(n))
  def noConcurrency(): StageExecutionConfig                    = copy(concurrency = None)
  def withRetryUncertain(retry: Boolean): StageExecutionConfig = copy(retryUncertain = retry)
  def withRetryFailed(retry: Boolean): StageExecutionConfig    = copy(retryFailed = retry)

  /**
    * Compute the delay before the next retry attempt. attempt is 1-origin (the number of attempts
    * made so far)
    */
  def retryDelayFor(attempt: Int): Long =
    val base =
      backoff match
        case StageExecutionConfig.BACKOFF_CONSTANT =>
          retryDelayMillis
        case StageExecutionConfig.BACKOFF_LINEAR =>
          retryDelayMillis * attempt
        case _ =>
          // exponential (default): base * 2^(attempt-1)
          retryDelayMillis * (1L << (attempt - 1).min(30))
    maxRetryDelayMillis.fold(base)(_.min(base))

end StageExecutionConfig

object StageExecutionConfig:
  val BACKOFF_CONSTANT    = "constant"
  val BACKOFF_LINEAR      = "linear"
  val BACKOFF_EXPONENTIAL = "exponential"

  /** Default polling interval of `wait until` event sensors */
  val DEFAULT_POLL_INTERVAL_MILLIS = 10_000L

  def fromConfigItems(items: List[ConfigItem]): StageExecutionConfig =
    items.foldLeft(StageExecutionConfig()) { (config, item) =>
      def durationMillis: Option[Long] =
        item.value match
          case d: DurationLiteral =>
            Some(d.toMillis)
          case _ =>
            None
      item.key.unquotedValue match
        case "retries" =>
          item.value match
            case l: LongLiteral =>
              config.withRetries(l.value.toInt)
            case _ =>
              config
        case "retry_delay" =>
          durationMillis.fold(config)(config.withRetryDelayMillis)
        case "backoff" =>
          item.value match
            case s: StringLiteral =>
              config.withBackoff(s.unquotedValue)
            case _ =>
              config
        case "max_retry_delay" =>
          durationMillis.fold(config)(config.withMaxRetryDelayMillis)
        case "timeout" =>
          durationMillis.fold(config)(config.withTimeoutMillis)
        case "heartbeat" =>
          durationMillis.fold(config)(config.withHeartbeatMillis)
        case "poll_interval" =>
          durationMillis.fold(config)(config.withPollIntervalMillis)
        case "concurrency" =>
          item.value match
            case l: LongLiteral =>
              config.withConcurrency(l.value.toInt)
            case _ =>
              config
        case "retry_uncertain" =>
          item.value match
            case b: BooleanLiteral =>
              config.withRetryUncertain(b.booleanValue)
            case _ =>
              config
        case "retry_failed" =>
          item.value match
            case b: BooleanLiteral =>
              config.withRetryFailed(b.booleanValue)
            case _ =>
              config
        case _ =>
          // Unknown or not-yet-supported properties are ignored by this executor
          config
      end match
    }

end StageExecutionConfig

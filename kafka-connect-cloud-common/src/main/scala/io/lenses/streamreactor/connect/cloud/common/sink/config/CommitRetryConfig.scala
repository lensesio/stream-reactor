/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

/**
 * Retry settings for the commit-chain Copy (`mvFile`) and Delete (`deleteFile`)
 * operations performed by `RetryingStorageInterface`.
 *
 * These are distinct from:
 *   - `connect.<prefix>.max.retries` / `connect.<prefix>.retry.interval` — Connect
 *     error-policy retries that apply to the whole `put()` invocation.
 *   - `connect.<prefix>.http.*` (GCS) — GAX/SDK retries at the HTTP layer.
 *
 * @param maxAttempts  Total attempts (initial + retries). 1 means no retry.
 * @param baseDelayMs  Delay before the first retry, in milliseconds.
 * @param multiplier   Backoff multiplier applied to each successive delay.
 * @param maxDelayMs   Upper bound on the inter-retry delay, in milliseconds.
 */
case class CommitRetryConfig(
  maxAttempts: Int,
  baseDelayMs: Long,
  multiplier:  Double,
  maxDelayMs:  Long,
)

object CommitRetryConfig {
  val DefaultMaxAttempts: Int    = 5
  val DefaultBaseDelayMs: Long   = 200L
  val DefaultMultiplier:  Double = 2.0
  val DefaultMaxDelayMs:  Long   = 5000L

  val Default: CommitRetryConfig = CommitRetryConfig(
    DefaultMaxAttempts,
    DefaultBaseDelayMs,
    DefaultMultiplier,
    DefaultMaxDelayMs,
  )
}

/**
 * Config key constants for commit-retry settings.
 * Mixed into each cloud's `ConfigDef` object alongside other key traits.
 */
trait CommitRetryConfigKeys extends WithConnectorPrefix {

  val COMMIT_RETRY_MAX_ATTEMPTS: String = s"$connectorPrefix.commit.retry.max.attempts"
  private val COMMIT_RETRY_MAX_ATTEMPTS_DOC: String =
    s"Total number of attempts (initial + retries) for the commit-chain Copy and Delete cloud operations. " +
      s"Set to 1 to disable in-process retries for these steps and rely solely on the crash-recovery path. " +
      s"Values > 1 allow transient network errors (TCP resets, EOF) to be retried in-process before failing the task."
  private val COMMIT_RETRY_MAX_ATTEMPTS_DEFAULT: Int = CommitRetryConfig.DefaultMaxAttempts

  val COMMIT_RETRY_BASE_DELAY_MS: String = s"$connectorPrefix.commit.retry.base.delay.ms"
  private val COMMIT_RETRY_BASE_DELAY_MS_DOC: String =
    s"Delay in milliseconds before the first commit-retry attempt."
  private val COMMIT_RETRY_BASE_DELAY_MS_DEFAULT: Long = CommitRetryConfig.DefaultBaseDelayMs

  val COMMIT_RETRY_MULTIPLIER: String = s"$connectorPrefix.commit.retry.multiplier"
  private val COMMIT_RETRY_MULTIPLIER_DOC: String =
    s"Exponential backoff multiplier applied to each successive inter-retry delay."
  private val COMMIT_RETRY_MULTIPLIER_DEFAULT: Double = CommitRetryConfig.DefaultMultiplier

  val COMMIT_RETRY_MAX_DELAY_MS: String = s"$connectorPrefix.commit.retry.max.delay.ms"
  private val COMMIT_RETRY_MAX_DELAY_MS_DOC: String =
    s"Upper bound on the inter-retry delay in milliseconds. Prevents unbounded backoff."
  private val COMMIT_RETRY_MAX_DELAY_MS_DEFAULT: Long = CommitRetryConfig.DefaultMaxDelayMs

  def addCommitRetrySettingsToConfigDef(configDef: ConfigDef): ConfigDef =
    configDef
      .define(
        COMMIT_RETRY_MAX_ATTEMPTS,
        Type.INT,
        COMMIT_RETRY_MAX_ATTEMPTS_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        COMMIT_RETRY_MAX_ATTEMPTS_DOC,
        "Commit Retry",
        1,
        ConfigDef.Width.SHORT,
        COMMIT_RETRY_MAX_ATTEMPTS,
      )
      .define(
        COMMIT_RETRY_BASE_DELAY_MS,
        Type.LONG,
        COMMIT_RETRY_BASE_DELAY_MS_DEFAULT,
        ConfigDef.Range.atLeast(0L),
        Importance.LOW,
        COMMIT_RETRY_BASE_DELAY_MS_DOC,
        "Commit Retry",
        2,
        ConfigDef.Width.SHORT,
        COMMIT_RETRY_BASE_DELAY_MS,
      )
      .define(
        COMMIT_RETRY_MULTIPLIER,
        Type.DOUBLE,
        COMMIT_RETRY_MULTIPLIER_DEFAULT,
        Importance.LOW,
        COMMIT_RETRY_MULTIPLIER_DOC,
        "Commit Retry",
        3,
        ConfigDef.Width.SHORT,
        COMMIT_RETRY_MULTIPLIER,
      )
      .define(
        COMMIT_RETRY_MAX_DELAY_MS,
        Type.LONG,
        COMMIT_RETRY_MAX_DELAY_MS_DEFAULT,
        ConfigDef.Range.atLeast(0L),
        Importance.LOW,
        COMMIT_RETRY_MAX_DELAY_MS_DOC,
        "Commit Retry",
        4,
        ConfigDef.Width.SHORT,
        COMMIT_RETRY_MAX_DELAY_MS,
      )
}

/**
 * Mixin for cloud-sink config builders to read commit-retry settings.
 *
 * Requires `getParsedValues: Map[String, _]` to be provided by the mixing-in
 * class (all concrete builders expose this). This is needed because
 * [[BaseSettings]] does not expose `getDouble`, but the values map exposes
 * it through [[ConfigParse.getDouble]].
 */
trait CommitRetrySettings extends BaseSettings with CommitRetryConfigKeys {

  def getParsedValues: Map[String, _]

  def getCommitRetryConfig: CommitRetryConfig =
    CommitRetryConfig(
      maxAttempts = getInt(COMMIT_RETRY_MAX_ATTEMPTS),
      baseDelayMs = getLong(COMMIT_RETRY_BASE_DELAY_MS),
      multiplier = ConfigParse.getDouble(getParsedValues, COMMIT_RETRY_MULTIPLIER)
        .getOrElse(CommitRetryConfig.DefaultMultiplier),
      maxDelayMs = getLong(COMMIT_RETRY_MAX_DELAY_MS),
    )
}

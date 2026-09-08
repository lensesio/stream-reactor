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
package io.lenses.streamreactor.connect.elastic.common.sink

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.dlq.RecordRef
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * Abstract base SinkTask for all elastic/opensearch connectors.
 * Subclasses must implement [[createWriter]], [[errorRetryIntervalKey]], [[progressEnabledKey]],
 * and provide the config-def parse step inside [[createWriter]].
 */
abstract class AbstractElasticSinkTask extends SinkTask with StrictLogging with JarManifestProvided {

  private var writer: Option[JsonBulkWriter] = None
  private[sink] val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false

  protected def asciiArtResource: String

  /**
   * Create the writer from the resolved config map. Also responsible for calling
   * `configDef.parse(conf)` BEFORE constructing settings — this surfaces missing
   * required keys as ConfigException at the start boundary.
   *
   * `dlqGate` reflects whether Kafka Connect has actually made a dead-letter queue available
   * for this task (see [[DlqGate.enabled]]) — implementations should pass it through to the
   * [[JsonBulkWriter]] they construct so permanently-failed bulk items can be reported to it.
   */
  protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter

  protected def errorRetryIntervalKey: String
  protected def progressEnabledKey:    String

  /**
   * Full config key for `bulk.item.errors` (connector-prefixed, e.g.
   * `connect.elastic.bulk.item.errors` / `connect.opensearch.bulk.item.errors`). ES6 has no
   * registered key (its bulk client never populates `BulkItemError.position`, so the mode stays
   * [[ItemErrorMode.Fail]]) — any string works, since an absent key in `confMap` defaults to
   * `fail` regardless.
   *
   * Read directly from `confMap`, not a parsed `ElasticCommonSettings`, so
   * [[validateItemErrorConfiguration]] can run — and its warnings fire — before [[createWriter]],
   * which connects to the cluster and can throw first.
   */
  protected def itemErrorModeConfigKey: String

  /** Full config key for `bulk.strict.item.errors` (connector-prefixed). See [[itemErrorModeConfigKey]]. */
  protected def strictItemErrorsConfigKey: String

  /**
   * Whether this connector implementation supports `bulk.item.errors` diversion (`drop`/`dlq`).
   * ES7 and OpenSearch override nothing (default `true`); ES6 overrides to `false` because
   * [[io.lenses.streamreactor.connect.elastic6.KElastic6BulkClient]] never populates
   * `BulkItemError.position` and [[io.lenses.streamreactor.connect.elastic6.config.ElasticSettings]]
   * always pins `itemErrorMode` to [[ItemErrorMode.Fail]].
   *
   * When `false`, [[validateItemErrorConfiguration]] emits a single WARN if the user set
   * `bulk.item.errors` to anything other than `fail`, telling them the setting is ignored, and
   * skips the mode-specific warnings that would otherwise imply the mode is active.
   */
  protected def supportsItemErrorDiversion: Boolean = true

  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, asciiArtResource)

    val conf    = if (context.configs().isEmpty) props else context.configs()
    val confMap = conf.asScala.toMap

    // Non-null errantRecordReporter alone doesn't mean a DLQ exists (see DlqGate).
    //
    // SinkTaskContext.errantRecordReporter() was added in Kafka Connect 2.6. On older runtimes
    // it throws NoSuchMethodError (a LinkageError, not caught by scala.util.Try/NonFatal) —
    // caught explicitly.
    val reporter =
      try Option(context.errantRecordReporter())
      catch {
        case _: NoSuchMethodError =>
          logger.debug(
            "SinkTaskContext.errantRecordReporter() is unavailable on this Kafka Connect runtime " +
              "(older than 2.6); per-record DLQ reporting is unavailable.",
          )
          None
      }
    val dlqGate = DlqGate.from(reporter, confMap)
    logger.info(
      s"DlqGate resolved: reporter=${dlqGate.reporter.isDefined} " +
        s"dlqTopicConfigured=${dlqGate.dlqTopicConfigured} enabled=${dlqGate.enabled}",
    )

    // Config-only: reads confMap/dlqGate alone, so it runs — and its WARN(s) are guaranteed to
    // be seen — before createWriter, which connects to the cluster and can throw.
    validateItemErrorConfiguration(confMap, dlqGate)

    val w = createWriter(confMap, dlqGate)
    logger.info(
      s"bulk.item.errors=${w.settings.itemErrorMode} resolved to effective mode ${w.effectiveItemErrorMode}.",
    )

    // Wire the retry interval into the context timeout when policy is RETRY
    w.settings.errorPolicy match {
      case RetryErrorPolicy() =>
        val retryInterval = conf.asScala.get(errorRetryIntervalKey).map(_.toLong).getOrElse(
          ElasticCommonConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
        )
        context.timeout(retryInterval)
      case _ =>
    }

    enableProgress = conf.asScala.get(progressEnabledKey).exists(_.toBoolean)
    writer         = Some(w)
  }

  /**
   * Warns at start() about drop/dlq misconfigurations, rather than at the first poison record.
   *
   * Reads `bulk.item.errors`/`bulk.strict.item.errors` directly from `confMap` via
   * [[itemErrorModeConfigKey]]/[[strictItemErrorsConfigKey]], not from a parsed
   * `ElasticCommonSettings`, so this runs — and its warnings are visible — before
   * [[createWriter]], which connects to the cluster and can throw. Applies the same defaulting
   * as [[io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode.fromString]] /
   * `ElasticCommonConfigConstants.BULK_ITEM_ERRORS_DEFAULT` / `BULK_STRICT_ITEM_ERRORS_DEFAULT`
   * would via a real `ConfigDef.parse`.
   */
  private def validateItemErrorConfiguration(confMap: Map[String, String], dlqGate: DlqGate): Unit = {
    val rawValue = confMap.getOrElse(itemErrorModeConfigKey, ElasticCommonConfigConstants.BULK_ITEM_ERRORS_DEFAULT)
    val itemErrorMode = ItemErrorMode.fromString(rawValue)

    // ES6: bulk.item.errors is not registered in ConfigDef and ElasticSettings always pins
    // itemErrorMode to Fail. Kafka Connect still passes unregistered properties through to the
    // task config map, so a user CAN set the key — but it has no effect. Warn once and skip the
    // mode-specific warnings that would otherwise imply the mode is active.
    if (!supportsItemErrorDiversion && itemErrorMode != ItemErrorMode.Fail) {
      logger.warn(
        s"bulk.item.errors=$rawValue is set but this connector does not support per-record diversion " +
          "(the bulk client does not expose per-item positions). The setting is ignored — the connector " +
          "will behave as bulk.item.errors=fail regardless.",
      )
      return
    }

    val strictItemErrors = confMap.get(strictItemErrorsConfigKey)
      .map(_.toBoolean)
      .getOrElse(ElasticCommonConfigConstants.BULK_STRICT_ITEM_ERRORS_DEFAULT)

    itemErrorMode match {
      case ItemErrorMode.Fail => // silent — identical to the pre-diversion behaviour
      case ItemErrorMode.Drop =>
        logger.warn(
          "bulk.item.errors=drop is active. Records permanently rejected by Elasticsearch/OpenSearch " +
            "(e.g. a mapping conflict) and records that cannot be prepared for indexing (e.g. a missing " +
            "header or unresolvable primary key) will be silently discarded — they are gone and cannot " +
            "be recovered. The task will keep running and offsets will advance past them. " +
            "Use bulk.item.errors=dlq if you need a recoverable copy of discarded records.",
        )
        if (!strictItemErrors) {
          logger.warn(
            "bulk.item.errors=drop and bulk.strict.item.errors=false are both set. " +
              "Records permanently rejected by Elasticsearch/OpenSearch are already silently discarded " +
              "by the bulk client (bulk.strict.item.errors=false) before bulk.item.errors is consulted — " +
              "so bulk.item.errors=drop adds no extra behaviour for those and its per-batch summary log " +
              "will not appear for them. Set bulk.strict.item.errors=true if you want bulk.item.errors=drop " +
              "to control what happens to rejected records. " +
              "Records that cannot be prepared for indexing (e.g. a missing header or unresolvable primary key) " +
              "are still discarded by bulk.item.errors=drop regardless of this setting.",
          )
        }
      case ItemErrorMode.Dlq =>
        // Each condition under which dlq degrades to fail (JsonBulkWriter.effectiveItemErrorMode
        // gates on dlqGate.enabled = reporter.isDefined && dlqTopicConfigured) gets its own WARN.
        if (!dlqGate.dlqTopicConfigured) {
          logger.warn(
            s"bulk.item.errors=dlq is set but ${DlqGate.DlqTopicNameConfigKey} is not configured or is empty. " +
              s"Failed records will not be sent to a dead-letter queue — the task will fail instead (same as bulk.item.errors=fail). " +
              s"Set ${DlqGate.DlqTopicNameConfigKey} to enable dead-letter queue routing.",
          )
        } else if (dlqGate.reporter.isEmpty) {
          logger.warn(
            "bulk.item.errors=dlq is set but this Kafka Connect runtime does not support per-record error reporting " +
              "(requires Kafka Connect 2.6 or later). Failed records will not be sent to a dead-letter queue — " +
              "the task will fail instead (same as bulk.item.errors=fail). Upgrade Kafka Connect to enable this.",
          )
        }
        if (!strictItemErrors) {
          logger.warn(
            "bulk.item.errors=dlq and bulk.strict.item.errors=false are both set. " +
              "Records permanently rejected by Elasticsearch/OpenSearch are already silently discarded " +
              "by the bulk client (bulk.strict.item.errors=false) before bulk.item.errors is consulted — " +
              "so those records will never reach the dead-letter queue. " +
              "Set bulk.strict.item.errors=true if you want rejected records to be routed to the dead-letter queue. " +
              "Records that cannot be prepared for indexing (e.g. a missing header or unresolvable primary key) " +
              "are still routed to the dead-letter queue by bulk.item.errors=dlq regardless of this setting.",
          )
        }
        // errors.tolerance defaults to "none" when unset — treat absent/blank the same as
        // explicit "none".
        val tolerance = confMap.get(DlqGate.ErrorsToleranceConfigKey)
          .map(_.trim.toLowerCase)
          .filter(_.nonEmpty)
          .getOrElse("none")
        if (tolerance == "none") {
          logger.warn(
            s"bulk.item.errors=dlq is set but ${DlqGate.ErrorsToleranceConfigKey} is 'none' (the default). " +
              s"With this setting Kafka Connect will fail the task after routing a record to the dead-letter queue, " +
              s"so the task cannot keep running. The record will still reach the dead-letter queue, but the task will stop. " +
              s"Set ${DlqGate.ErrorsToleranceConfigKey}=all for the task to continue after routing failed records.",
          )
        }
    }
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector
    // Set[RecordRef] (topic/partition/offset), not Set[SinkRecord]: SinkRecord.hashCode/equals
    // recurse through the whole payload Struct and Schema, and this set is checked once per
    // record in the batch below, on every put(). See RecordRef.
    val divertedRefs = writer.map(_.write(seq)).getOrElse(Set.empty[RecordRef])

    if (enableProgress) {
      val delivered =
        if (divertedRefs.isEmpty) seq else seq.filterNot(r => divertedRefs.contains(RecordRef.of(r)))
      progressCounter.update(delivered)
    }
  }

  override def stop(): Unit = {
    logger.info("Stopping sink connector.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit =
    logger.info("Flushing sink")
}

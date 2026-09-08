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
  private val progressCounter = new ProgressCounter
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
   * registered key for this (its bulk client never populates `BulkItemError.position`, so the
   * mode always stays [[ItemErrorMode.Fail]]) — return any string; an absent key in `confMap`
   * resolves to the `fail` default either way, which matches ES6's actual pinned behaviour.
   *
   * Read directly from `confMap` here (not via a parsed `ElasticCommonSettings`) so
   * [[validateItemErrorConfiguration]] can run, and its warnings can fire, BEFORE
   * [[createWriter]] — which connects to the cluster and can throw before those warnings would
   * otherwise ever be seen.
   */
  protected def itemErrorModeConfigKey: String

  /** Full config key for `bulk.strict.item.errors` (connector-prefixed). See [[itemErrorModeConfigKey]]. */
  protected def strictItemErrorsConfigKey: String

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
    // be seen — BEFORE createWriter, which connects to the cluster and can throw. Previously
    // this ran after createWriter, so any misconfiguration warning went unseen whenever the
    // cluster was unreachable at start().
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
   * [[itemErrorModeConfigKey]]/[[strictItemErrorsConfigKey]] rather than from a parsed
   * `ElasticCommonSettings`, specifically so this can run — and its warnings be guaranteed
   * visible — before [[createWriter]], which connects to the cluster and can throw. Mirrors
   * [[io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode.fromString]] /
   * `ElasticCommonConfigConstants.BULK_ITEM_ERRORS_DEFAULT` / `BULK_STRICT_ITEM_ERRORS_DEFAULT`
   * for the same defaulting behaviour a real `ConfigDef.parse` would apply.
   */
  private def validateItemErrorConfiguration(confMap: Map[String, String], dlqGate: DlqGate): Unit = {
    val itemErrorMode = ItemErrorMode.fromString(
      confMap.getOrElse(itemErrorModeConfigKey, ElasticCommonConfigConstants.BULK_ITEM_ERRORS_DEFAULT),
    )
    val strictItemErrors = confMap.get(strictItemErrorsConfigKey)
      .map(_.toBoolean)
      .getOrElse(ElasticCommonConfigConstants.BULK_STRICT_ITEM_ERRORS_DEFAULT)

    itemErrorMode match {
      case ItemErrorMode.Fail => // silent — identical to the pre-diversion behaviour
      case ItemErrorMode.Drop =>
        logger.warn(
          "bulk.item.errors=drop is set: permanently-failed bulk items (HTTP 400, e.g. a mapper " +
            "conflict) will be discarded and offsets will advance past them. This is a deliberate " +
            "data-loss trade-off in exchange for keeping the task running; use bulk.item.errors=dlq " +
            "instead if you need a durable record of what was discarded.",
        )
        if (!strictItemErrors) {
          logger.warn(
            "bulk.item.errors=drop is set together with bulk.strict.item.errors=false. Both discard " +
              "poison records, but bulk.strict.item.errors=false already discards them inside the bulk " +
              "client (legacy tolerant mode) before bulk.item.errors is consulted, so bulk.item.errors=drop " +
              "has no additional effect and its per-batch drop summary logging will not appear. Set " +
              "bulk.strict.item.errors=true if you want bulk.item.errors=drop to be what discards them.",
          )
        }
      case ItemErrorMode.Dlq =>
        // Both branches below are conditions under which dlq degrades to fail
        // (JsonBulkWriter.effectiveItemErrorMode gates on dlqGate.enabled = reporter.isDefined &&
        // dlqTopicConfigured). Each degrade path must have its own WARN, so a silent
        // dlq -> fail is never possible.
        if (!dlqGate.dlqTopicConfigured) {
          logger.warn(
            s"bulk.item.errors=dlq is set but ${DlqGate.DlqTopicNameConfigKey} is not configured " +
              "(or is empty); dlq cannot take effect and the connector will behave as if set to fail.",
          )
        } else if (dlqGate.reporter.isEmpty) {
          logger.warn(
            "bulk.item.errors=dlq is set but Kafka Connect did not provide an ErrantRecordReporter " +
              "for this task (typically a Connect runtime older than 2.6); dlq cannot take effect and " +
              "the connector will behave as if set to fail.",
          )
        }
        if (!strictItemErrors) {
          logger.warn(
            "bulk.item.errors=dlq is set together with bulk.strict.item.errors=false; in tolerant " +
              "mode per-item bulk failures are never surfaced to the writer, so nothing will ever be " +
              "reported to the dead-letter queue.",
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
            s"bulk.item.errors=dlq is set together with ${DlqGate.ErrorsToleranceConfigKey}=none " +
              s"(the default when ${DlqGate.ErrorsToleranceConfigKey} is unset); " +
              "Kafka Connect throws out of the DLQ report call once tolerance is exceeded, so dlq " +
              "cannot keep the task running under this setting (the record still reaches the DLQ, but " +
              s"the task then fails fast). Set ${DlqGate.ErrorsToleranceConfigKey}=all for dlq to work.",
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

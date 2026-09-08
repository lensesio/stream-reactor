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
package io.lenses.streamreactor.connect.elastic.common.writer

import cats.implicits.toBifunctorOps
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.WriteModeEnum
import io.lenses.kcql.Kcql
import io.lenses.sql.Field
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
import io.lenses.streamreactor.connect.elastic.common.KcqlValues
import io.lenses.streamreactor.connect.elastic.common.NullValueBehavior
import io.lenses.streamreactor.connect.elastic.common.Transform
import io.lenses.streamreactor.connect.elastic.common.TransformAndExtractPK
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemErrorClassifier
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
import io.lenses.streamreactor.connect.elastic.common.dlq.DivertOutcome
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqKey
import io.lenses.streamreactor.connect.elastic.common.dlq.ErrantRecordHandler
import io.lenses.streamreactor.connect.elastic.common.dlq.RecordRef
import io.lenses.streamreactor.connect.elastic.common.indexname.CreateIndex
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.jdk.CollectionConverters._

class JsonBulkWriter(
  client:       KBulkClient,
  val settings: ElasticCommonSettings,
  dlqGate:      DlqGate = DlqGate.disabled,
) extends ErrorHandler
    with StrictLogging {

  logger.info("Initialising Json bulk writer")

  initialize(settings.taskRetries, settings.errorPolicy)

  private val errantRecordHandler = new ErrantRecordHandler(dlqGate, settings.dlqReportTimeoutMs)

  /**
   * Mode in effect, resolved once at construction. [[ItemErrorMode.Dlq]] degrades to
   * [[ItemErrorMode.Fail]] when no DLQ is available (see [[DlqGate.enabled]]).
   * [[ItemErrorMode.Drop]] is never degraded.
   *
   * Public so callers (e.g. [[io.lenses.streamreactor.connect.elastic.common.sink.AbstractElasticSinkTask]])
   * can log the mode ACTUALLY in effect at `start()`, not just the raw configured value — a
   * `dlq` -> `fail` degrade is otherwise only visible via the WARN(s) from
   * `AbstractElasticSinkTask.validateItemErrorConfiguration`.
   */
  val effectiveItemErrorMode: ItemErrorMode = settings.itemErrorMode match {
    case ItemErrorMode.Dlq if !dlqGate.enabled => ItemErrorMode.Fail
    case other                                 => other
  }
  private def effectiveMode: ItemErrorMode = effectiveItemErrorMode

  /** True when [[effectiveMode]] is [[ItemErrorMode.Drop]] or [[ItemErrorMode.Dlq]]. */
  private def divertActive: Boolean = effectiveMode != ItemErrorMode.Fail

  // Eagerly invoke the client's start lifecycle hook so any cluster-compatibility
  // failures (e.g. pointing at an Elasticsearch cluster) are surfaced at SinkTask.start,
  // not on the first batch of records.
  client.start().fold(throw _, identity)

  // Warn about WITHDOCTYPE clauses only when the backend does not honour them.
  // Elasticsearch 6 uses document types and will honour the clause; ES7 and OpenSearch dropped
  // mapping types and silently ignore it.  Emitting the warning for ES6 would be misleading.
  private val docTypeKcqls = settings.kcqls.filter(k => Option(k.getDocType).exists(_.nonEmpty))
  if (docTypeKcqls.nonEmpty && !client.supportsDocumentType) {
    logger.warn(
      s"The following KCQLs use WITHDOCTYPE which is silently ignored on this connector: ${docTypeKcqls.map(_.getTarget).mkString(", ")}",
    )
  }

  // Validate write modes upfront — fail fast on unsupported modes
  settings.kcqls.foreach { kcql =>
    kcql.getWriteMode match {
      case WriteModeEnum.INSERT | WriteModeEnum.UPSERT => // ok
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported KCQL write mode: $other; only INSERT and UPSERT are supported",
        )
    }
  }

  // Auto-create indexes. Deduplicate by resolved index name so each index is created at most once.
  settings.kcqls
    .filter(_.isAutoCreate)
    .flatMap { kcql =>
      CreateIndex.getIndexNameForAutoCreate(kcql).fold(throw _, Some(_))
    }
    .distinct
    .foreach { indexName =>
      client.createIndex(indexName).fold(throw _, identity)
    }

  private val topicKcqlMap: Map[String, Seq[Kcql]] = settings.kcqls.groupBy(_.getSource)

  private val kcqlMap: util.IdentityHashMap[Kcql, KcqlValues] = {
    val m = new util.IdentityHashMap[Kcql, KcqlValues]()
    settings.kcqls.foreach { kcql =>
      m.put(
        kcql,
        KcqlValues(
          kcql.getFields.asScala.map(f =>
            Field(f.getName, f.getAlias, Option(f.getParentFields).map(_.asScala.toVector).orNull),
          ).toSeq,
          kcql.getIgnoredFields.asScala.map(f =>
            Field(f.getName, f.getAlias, Option(f.getParentFields).map(_.asScala.toVector).orNull),
          ).toSeq,
          kcql.getPrimaryKeys.asScala.map { pk =>
            val path = Option(pk.getParentFields).map(_.asScala.toVector).getOrElse(Vector.empty)
            path :+ pk.getName
          }.toSeq,
          NullValueBehavior.fromString(fetchNullValueBehaviorProperty(kcql)),
        ),
      )
    }
    m
  }

  private def fetchNullValueBehaviorProperty(kcql: Kcql): String =
    kcql.getProperties.asScala.keys
      .find(_.toLowerCase == ElasticCommonConfigConstants.BEHAVIOR_ON_NULL_VALUES_PROPERTY)
      .map(kcql.getProperties.get)
      .orNull

  def close(): Unit = client.close()

  /**
   * Writes a batch of records. Returns the subset diverted (dropped or DLQ'd) rather than
   * written, identified by [[RecordRef]] (Kafka's own topic/partition/offset coordinates, not
   * the `SinkRecord` itself — see [[RecordRef]]) — callers should exclude these from
   * delivered-record accounting.
   *
   * Dedup tracking is scoped to this call: a fresh tracker per `write()`, so diversions are not
   * suppressed across a redelivered `put()` (at-least-once; see [[DlqKey]]). Note that a single
   * bulk response mixing a divertible permanent error with a retriable one defers diversion
   * entirely rather than diverting the permanent subset every redelivery — see
   * [[handleBulkResult]] — so at-least-once here is bounded by "at most once per distinct
   * *whole-`put()`* redelivery", not "once per retriable sub-attempt".
   */
  def write(records: Vector[SinkRecord]): Set[RecordRef] =
    if (records.isEmpty) {
      logger.debug("No records received.")
      Set.empty
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      // Fresh, empty tracker per write() call — never shared across calls or instances, since
      // it's mutable and non-thread-safe. The allocation is negligible next to the JSON
      // transform and HTTP round trip `insert` performs per batch.
      val reportedThisPut = mutable.LinkedHashMap.empty[DlqKey, SinkRecord]
      insert(grouped, reportedThisPut)
      if (reportedThisPut.nonEmpty) {
        logger.info(
          s"Diverted ${reportedThisPut.size} record(s) (bulk.item.errors=${effectiveMode.configValue}) during this put().",
        )
      }
      reportedThisPut.keys.map(k => RecordRef(k.topic, k.partition, k.offset)).toSet
    }

  private def insert(
    records:         Map[String, Vector[SinkRecord]],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Unit = {
    logger.info(s"Inserting ${records.size} record groups")
    records.foreach {
      case (topic, sinkRecords) =>
        logger.debug(s"Inserting ${sinkRecords.size} records from $topic")
        val kcqls: Seq[Kcql] = topicKcqlMap.getOrElse(
          topic,
          throw new IllegalArgumentException(
            s"$topic hasn't been configured in KCQL. Configured topics is ${topicKcqlMap.keys.mkString(",")}",
          ),
        )
        kcqls.foreach { kcql: Kcql =>
          val kcqlValue: KcqlValues = kcqlMap.get(kcql)
          sinkRecords.grouped(settings.batchSize).foreach { batch =>
            val opsWithRecords: Vector[(BulkOp, SinkRecord)] =
              batch.flatMap { r =>
                Try(processRecord(topic, kcql, kcqlValue, r)) match {
                  case Success(opt) => opt.map(_ -> r)
                  // Only PerRecordWriterFailure is eligible for diversion — everything else
                  // (schema/config/KCQL-level failures such as a missing header field, PK
                  // extraction failing, or an unsupported write mode) is a property of the whole
                  // topic/KCQL, not of this one record, and must still fail the task. See
                  // PerRecordWriterFailure.
                  case Failure(e: PerRecordWriterFailure) if divertActive =>
                    val target = Option(kcql.getTarget).getOrElse(topic)
                    if (errantRecordHandler.divertOne(r, target, e, effectiveMode, reportedThisPut)) {
                      logger.warn(
                        s"Writer-stage failure for ${r.topic()}-${r.kafkaPartition()}-${r.kafkaOffset()} " +
                          s"diverted (bulk.item.errors=${effectiveMode.configValue}) instead of failing the task: ${e.getMessage}",
                      )
                      None
                    } else {
                      throw e
                    }
                  case Failure(e) => throw e
                }
              }
            if (opsWithRecords.nonEmpty) {
              val ops = opsWithRecords.map(_._1)
              val bulkTry = client.bulk(ops) match {
                case Success(result) => handleBulkResult(result, opsWithRecords, reportedThisPut)
                case Failure(error)  => handleTransportFailure(error)
              }
              handleTry(bulkTry)
            }
          }
        }
    }
  }

  /**
   * Classifies a successful bulk response and, when diversion is active, diverts divertible
   * permanent item errors per [[effectiveMode]].
   *
   *  - Permanent errors split into per-document rejections (HTTP 400, see
   *    [[BulkItemErrorClassifier.isDivertible]]) and batch-wide failures (403/404/5xx and the
   *    batch-wide 400s). Any batch-wide failure blocks diversion for the whole batch — throws
   *    for the full error set.
   *  - Diversion is DEFERRED whenever a retriable (429/409) remainder is present in the SAME
   *    response, even if every other error in it is divertible: nothing is diverted, and the
   *    full error set throws for redelivery. This is deliberate, not a missed case — diverting
   *    the permanent subset here and throwing for the retriable remainder would mean the same
   *    permanent record(s) get diverted again on every redelivery attempt while the retriable
   *    condition persists (unbounded amplification: N redeliveries -> N DLQ reports / N drop
   *    log lines for the same record). Deferring means a mixed batch diverts at most once — on
   *    whichever attempt eventually comes back with ONLY divertible errors (the retriable
   *    condition having cleared) — and, in the pathological case where the retriable condition
   *    never clears, the task fails on retry-budget exhaustion having diverted nothing, which is
   *    the same outcome `fail` mode always had for that batch.
   */
  private def handleBulkResult(
    result:          BulkResult,
    opsWithRecords:  Vector[(BulkOp, SinkRecord)],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Try[BulkResult] =
    if (!result.errors) {
      Success(result)
    } else {
      val (retriable, nonRetriable) = result.itemErrors.partition(BulkItemErrorClassifier.isRetriable)
      val (perDocumentRejections, batchWideFailures) =
        nonRetriable.partition(BulkItemErrorClassifier.isDivertible)
      if (perDocumentRejections.isEmpty || batchWideFailures.nonEmpty || retriable.nonEmpty || !divertActive) {
        // Nothing divertible, a batch-wide error is present, a retriable remainder is present
        // (diversion deferred to a clean attempt — see scaladoc above), or diversion is
        // inactive: throw for the full error set (pre-diversion behaviour).
        Failure(BulkItemErrorClassifier.exceptionFor(result.itemErrors))
      } else {
        errantRecordHandler.divertPermanent(
          perDocumentRejections,
          opsWithRecords,
          effectiveMode,
          reportedThisPut,
        ) match {
          case DivertOutcome.Diverted    => Success(result)
          case DivertOutcome.NotDiverted => Failure(BulkItemErrorClassifier.exceptionFor(result.itemErrors))
        }
      }
    }

  /**
   * Classifies a transport-level failure (connection error, timeout, etc — no per-item
   * response received at all).
   *
   * Reclassified as [[RetriableIntegrityException]]:
   *  - A bare `RuntimeException` is swallowed by NOOP with only a WARN — offsets advance past
   *    unwritten records. Avoided by using a `FatalConnectException` subtype.
   *  - Not swallowed by NOOP, but still retried under `error.policy=RETRY` (unlike a plain
   *    `FatalConnectException`).
   *  - Never diverted: no per-document information exists to attribute the failure to
   *    specific records.
   *
   * [[RetriableIntegrityException]]'s own scaladoc frames retry-safety as "no partial state was
   * cached" — that framing fits its original cloud-sink use case but not quite this one: a bulk
   * transport failure is exactly the case where the cluster MAY have partially applied the
   * request. Retrying here is still safe, but for a different reason — INSERT/UPSERT ops are
   * keyed by document `id`, so re-sending an already-written document overwrites it identically
   * (idempotent), and DELETE is naturally idempotent. That idempotency, not "nothing was
   * cached", is what makes at-least-once redelivery of a whole batch safe.
   */
  private def handleTransportFailure(error: Throwable): Try[BulkResult] = {
    val classified = error match {
      case fatal: FatalConnectException => fatal
      case other => new RetriableIntegrityException(s"Bulk write failed: ${other.getMessage}", other)
    }
    Failure(classified)
  }

  private def removeIgnoredFields(node: JsonNode, ignoredFields: Seq[Field]): JsonNode =
    if (ignoredFields.isEmpty || !node.isObject) node
    else {
      val copy: ObjectNode = node.deepCopy[ObjectNode]()
      ignoredFields.foreach { f =>
        val parents = if (f.parents == null) Vector.empty[String] else f.parents
        val parent: Option[ObjectNode] = parents.foldLeft(Option(copy: JsonNode)) {
          case (Some(n), key) if n.isObject => Option(n.get(key)).filter(_.isObject)
          case _                            => None
        }.collect { case o: ObjectNode => o }
        parent.foreach(_.remove(f.name))
      }
      copy
    }

  private def processRecord(
    topic:     String,
    kcql:      Kcql,
    kcqlValue: KcqlValues,
    r:         SinkRecord,
  ): Option[BulkOp] = {
    val i            = CreateIndex.getIndexName(kcql, r).leftMap(throw _).merge
    val documentType = Option(kcql.getDocType)
    val (rawJson: Option[JsonNode], pks: Seq[Any]) =
      if (kcqlValue.primaryKeysPath.isEmpty) {
        (Transform(kcqlValue.fields, r.valueSchema(), r.value(), kcql.hasRetainStructure), Seq.empty[Any])
      } else {
        TransformAndExtractPK(
          kcqlValue,
          r.valueSchema(),
          r.value(),
          kcql.hasRetainStructure,
          r.keySchema(),
          r.key(),
          r.headers(),
        )
      }
    val json     = rawJson.map(removeIgnoredFields(_, kcqlValue.ignoredFields))
    val idFromPk = pks.mkString(settings.pkJoinerSeparator)

    json.filterNot(_.isEmpty) match {
      case Some(value) =>
        kcql.getWriteMode match {
          case WriteModeEnum.INSERT =>
            Some(
              InsertOp(
                index        = i,
                id           = if (idFromPk.isEmpty) autoGenId(r) else idFromPk,
                json         = value,
                pipeline     = Option(kcql.getPipeline),
                documentType = documentType,
              ),
            )

          case WriteModeEnum.UPSERT =>
            require(pks.nonEmpty, "Error extracting primary keys")
            Some(UpsertOp(index = i, id = idFromPk, json = value, documentType = documentType))

          case other =>
            throw new IllegalArgumentException(
              s"Unsupported KCQL write mode: $other; only INSERT and UPSERT are supported",
            )
        }
      case None =>
        handleTombstone(topic, kcqlValue, r, i, idFromPk, documentType)
    }
  }

  private def handleTombstone(
    topic:        String,
    kcqlValue:    KcqlValues,
    r:            SinkRecord,
    i:            String,
    idFromPk:     String,
    documentType: Option[String],
  ): Option[DeleteOp] =
    kcqlValue.behaviorOnNullValues match {
      case NullValueBehavior.DELETE =>
        val identifier = if (idFromPk.isEmpty) autoGenId(r) else idFromPk
        logger.debug(
          s"Deleting tombstone record: ${r.topic()} ${r.kafkaPartition()} ${r.kafkaOffset()}. Index: $i, Identifier: $identifier",
        )
        Option.apply(DeleteOp(index = i, id = identifier, documentType = documentType))

      case NullValueBehavior.FAIL =>
        logger.error(
          s"Tombstone record received ${r.topic()} ${r.kafkaPartition()} ${r.kafkaOffset()}. $topic KCQL mapping is configured to fail on tombstone records.",
        )
        // TombstoneRejectedException (not a bare IllegalStateException): tombstone-ness is a
        // per-record property, so this is the one writer-stage failure eligible for diversion
        // under bulk.item.errors=drop/dlq. See PerRecordWriterFailure.
        throw new TombstoneRejectedException(
          s"$topic KCQL mapping is configured to fail on tombstone records.",
        )

      case NullValueBehavior.IGNORE =>
        logger.info(
          s"Ignoring tombstone record received. for ${r.topic()} ${r.kafkaPartition()} ${r.kafkaOffset()}.",
        )
        None
    }

  def autoGenId(record: SinkRecord): String = {
    val pks: Seq[Any] = Seq(record.topic(), record.kafkaPartition(), record.kafkaOffset())
    pks.mkString(settings.pkJoinerSeparator)
  }
}

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
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorDlqMode
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqKey
import io.lenses.streamreactor.connect.elastic.common.dlq.ErrantRecordHandler
import io.lenses.streamreactor.connect.elastic.common.dlq.ReportOutcome
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

  private val errantRecordHandler = new ErrantRecordHandler(dlqGate)

  /**
   * True only when a real DLQ is available (see [[DlqGate.enabled]]) AND the connector is
   * configured to use it for permanently-failed items. When false, behaviour is byte-for-byte
   * identical to the pre-DLQ code path: any item error (permanent or, if unresolved, retriable)
   * throws via [[io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemErrorClassifier]].
   */
  private def skipModeActive: Boolean =
    dlqGate.enabled && settings.itemErrorDlqMode == ItemErrorDlqMode.Skip

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
   * Writes a batch of records. Returns the subset of `records` that were reported to the
   * dead-letter queue instead of being written — callers (e.g. progress/metrics counters)
   * should exclude these from "successfully delivered" accounting.
   *
   * The returned set, and the internal dedup tracking that produces it, are scoped to this
   * single call: a fresh tracker is created per `write()`, so it does not suppress duplicate
   * DLQ reports across a redelivered `put()` (Kafka Connect's own DLQ semantics are
   * at-least-once; see [[DlqKey]]).
   */
  def write(records: Vector[SinkRecord]): Set[SinkRecord] =
    if (records.isEmpty) {
      logger.debug("No records received.")
      Set.empty
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped         = records.groupBy(_.topic())
      val reportedThisPut = mutable.LinkedHashMap.empty[DlqKey, SinkRecord]
      insert(grouped, reportedThisPut)
      if (reportedThisPut.nonEmpty) {
        logger.info(s"Reported ${reportedThisPut.size} record(s) to the dead-letter queue during this put().")
      }
      reportedThisPut.values.toSet
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
                  case Failure(e) =>
                    if (
                      skipModeActive &&
                      errantRecordHandler.reportOne(r, Option(kcql.getTarget).getOrElse(topic), e, reportedThisPut)
                    ) {
                      logger.warn(
                        s"Writer-stage failure for ${r.topic()}-${r.kafkaPartition()}-${r.kafkaOffset()} " +
                          s"reported to the dead-letter queue instead of failing the task: ${e.getMessage}",
                      )
                      None
                    } else {
                      throw e
                    }
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
   * Classifies a successful (non-transport-failed) bulk response and, in skip mode, attempts to
   * divert DLQ-eligible permanent item errors to the DLQ.
   *
   * Permanent (non-retriable) errors are further split into DLQ-eligible (HTTP 400 — a
   * per-document rejection, see [[BulkItemErrorClassifier.isDlqEligible]]) and everything else
   * ("not eligible": 403/404/5xx conditions that affect the whole batch regardless of content,
   * e.g. `cluster_block_exception`, `security_exception`, `index_not_found_exception`). If ANY
   * not-eligible error is present, nothing in the batch is diverted — the whole call throws for
   * the full error set, exactly as it did before this feature existed, because a batch-wide
   * condition means the other "failures" in the same response are not safe to attribute to
   * individual documents.
   *
   * Once there is at least one DLQ-eligible error and no not-eligible error, the eligible set is
   * reported and only a genuinely retriable remainder (429/409/etc — left on the retry path)
   * causes a throw for redelivery of the batch. Because the dedup tracker (`reportedThisPut`) is
   * scoped to a single `put()` call, a retriable remainder that triggers redelivery means the
   * SAME DLQ-eligible errors will be reported to the DLQ again on each redelivery attempt, until
   * the retriable remainder clears or the retry budget is exhausted — DLQ delivery here is
   * at-least-once, matching Kafka Connect's own DLQ semantics (see [[DlqKey]]).
   */
  private def handleBulkResult(
    result:          BulkResult,
    opsWithRecords:  Vector[(BulkOp, SinkRecord)],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Try[BulkResult] =
    if (!result.errors) {
      Success(result)
    } else {
      val (retriable, permanent)     = result.itemErrors.partition(BulkItemErrorClassifier.isRetriable)
      val (dlqEligible, notEligible) = permanent.partition(BulkItemErrorClassifier.isDlqEligible)
      if (dlqEligible.isEmpty || notEligible.nonEmpty || !skipModeActive) {
        // Either nothing to divert (a pure-retriable batch, left on the retry path), a
        // batch-wide non-400 condition is present (never partially divert a batch — see
        // BulkItemErrorClassifier.isDlqEligible), or skip mode is not active: identical to the
        // pre-DLQ behaviour, throw for the full error set.
        Failure(BulkItemErrorClassifier.exceptionFor(result.itemErrors))
      } else {
        errantRecordHandler.reportPermanent(dlqEligible, opsWithRecords, reportedThisPut) match {
          case ReportOutcome.Reported if retriable.isEmpty =>
            Success(result)
          case ReportOutcome.Reported =>
            logger.warn(
              s"Reported ${dlqEligible.size} permanently-failed record(s) to the DLQ; " +
                s"${retriable.size} retriable error(s) remain in this batch and will fail it for re-delivery.",
            )
            Failure(BulkItemErrorClassifier.exceptionFor(retriable))
          case ReportOutcome.NotReported =>
            Failure(BulkItemErrorClassifier.exceptionFor(result.itemErrors))
        }
      }
    }

  /**
   * Classifies a failure from the bulk call itself (transport-level: connection errors, HTTP
   * transport errors, timeouts, etc — anything that prevented us from getting a per-item
   * response at all).
   *
   * Deliberately reclassified as [[RetriableIntegrityException]] rather than left as a plain
   * exception or promoted to a bare [[FatalConnectException]]:
   *  - A bare `RuntimeException` (the pre-existing behaviour) is swallowed by
   *    `NoopErrorPolicy` with only a WARN log — offsets advance past records that were never
   *    written and never reported anywhere. That silent-loss hole predates this change and is
   *    closed here.
   *  - A `RetriableIntegrityException` is NOT swallowed by NOOP (fail-fast, closing the hole
   *    above) but — critically — IS still retried under `error.policy=RETRY` while the retry
   *    budget remains, exactly like a 429/409 item error today. A transport blip (timeout,
   *    connection reset) is the common case here and is usually safe and likely to succeed on
   *    retry; promoting it to a plain `FatalConnectException` would have silently disabled that
   *    retry behaviour.
   *  - Deliberately NOT reported to the DLQ, even in skip mode: unlike a per-item bulk error, a
   *    whole-call transport failure gives no information about which (if any) individual
   *    documents are actually bad. Treating a transient network error as "these are all poison"
   *    would dump perfectly good documents into the DLQ when a cheap retry would have written
   *    them cleanly.
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
        throw new IllegalStateException(
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

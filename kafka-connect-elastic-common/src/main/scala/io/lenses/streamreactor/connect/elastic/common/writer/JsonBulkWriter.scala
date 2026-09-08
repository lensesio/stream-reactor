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
import io.lenses.kcql.targettype.TargetType
import io.lenses.sql.Field
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
import io.lenses.streamreactor.common.utils.CyclopsToScalaEither.convertToScalaEither
import io.lenses.streamreactor.connect.elastic.common.KafkaMessageParts
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
import io.lenses.streamreactor.connect.elastic.common.dlq.WriterStageFailure
import io.lenses.streamreactor.connect.elastic.common.indexname.CreateIndex
import io.lenses.streamreactor.connect.elastic.common.indexname.CustomIndexName
import io.lenses.streamreactor.connect.elastic.common.indexname.InvalidCustomIndexNameException
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

  // Validate write modes upfront — fail fast on unsupported modes. Pure KCQL-only check, no
  // cluster access — deliberately placed BEFORE client.start()/AUTOCREATE below, alongside
  // topicKcqlMap/kcqlMap, so a bad KCQL fails before any cluster side effect (a probe connection,
  // or an AUTOCREATE index actually being created). See kcqlMap's own scaladoc for the rest of
  // the KCQL-only validation this groups with.
  settings.kcqls.foreach { kcql =>
    kcql.getWriteMode match {
      case WriteModeEnum.INSERT | WriteModeEnum.UPSERT => // ok
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported KCQL write mode: $other; only INSERT and UPSERT are supported",
        )
    }
  }

  private val topicKcqlMap: Map[String, Seq[Kcql]] = settings.kcqls.groupBy(_.getSource)

  /**
   * Every KCQL-only input `processRecord` reads (target, target type, index suffix, primary key
   * paths, write mode) is resolved and validated exactly ONCE, here, rather than per record —
   * both for the hot-path cost of re-parsing them (`Kcql.getTargetType()` re-parses the target
   * string, `CustomIndexName.parseIndexName` re-parses `WITHINDEXSUFFIX` char-by-char, on every
   * call) and because anything that fails here fails identically for every record on the topic:
   * unlike record-content failures (a missing header, an unresolvable value path, a PK that
   * won't convert to the declared type), which are all divertible under
   * `bulk.item.errors=drop`/`dlq` (see `insert` below), a KCQL/config-level fault must still fail
   * the task in every mode, or the entire stream would be silently discarded one record at a
   * time while the task reported itself healthy.
   *
   * Deliberately placed BEFORE `client.start()` below: this is pure KCQL/config validation with
   * no cluster dependency, so a bad KCQL must fail before the writer ever probes the cluster or
   * (via AUTOCREATE, also below) creates an index there.
   */
  private val kcqlMap: util.IdentityHashMap[Kcql, KcqlValues] = {
    val m = new util.IdentityHashMap[Kcql, KcqlValues]()
    settings.kcqls.foreach { kcql =>
      val target = Option(kcql.getTarget).getOrElse(kcql.getSource)

      val primaryKeysPath: Seq[Vector[String]] = kcql.getPrimaryKeys.asScala.map { pk =>
        val path = Option(pk.getParentFields).map(_.asScala.toVector).getOrElse(Vector.empty)
        path :+ pk.getName
      }.toSeq

      // A PK path of "_header.a.b" fails identically for EVERY record on the topic — headers
      // only support single-level lookups (TransformAndExtractPK.extractPrimaryKey enforces the
      // same rule per record, defensively, but this writer never reaches it with such a path).
      primaryKeysPath.foreach { path =>
        if (path.head == KafkaMessageParts.Header && path.tail.size != 1) {
          throw new IllegalArgumentException(
            s"KCQL targeting '$target': invalid field selection for '${path.mkString(".")}'. " +
              "Headers lookup only supports single-level keys. Nested header keys are not supported.",
          )
        }
      }

      // UPSERT with no PK declared fails every record identically — it's a KCQL authoring
      // mistake, not a per-record condition. (Write mode itself is already validated above, so
      // only INSERT/UPSERT can reach this point.)
      if (kcql.getWriteMode == WriteModeEnum.UPSERT && primaryKeysPath.isEmpty) {
        throw new IllegalArgumentException(
          s"KCQL targeting '$target' uses UPSERT but declares no primary keys; UPSERT requires " +
            "at least one PK. Error extracting primary keys.",
        )
      }

      val targetType: TargetType = convertToScalaEither(kcql.getTargetType).fold(throw _, identity)

      m.put(
        kcql,
        KcqlValues(
          kcql.getFields.asScala.map(f =>
            Field(f.getName, f.getAlias, Option(f.getParentFields).map(_.asScala.toVector).orNull),
          ).toSeq,
          kcql.getIgnoredFields.asScala.map(f =>
            Field(f.getName, f.getAlias, Option(f.getParentFields).map(_.asScala.toVector).orNull),
          ).toSeq,
          primaryKeysPath,
          NullValueBehavior.fromString(fetchNullValueBehaviorProperty(kcql)),
          targetType,
          resolveIndexSuffix(kcql, target),
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

  /**
   * Parses `WITHINDEXSUFFIX` once at construction. Forcing `.toString` here surfaces an invalid
   * pattern at `start()` rather than on the first record. The cached `CustomIndexName` still
   * renders a fresh date/time on each per-record `.toString` — see `CreateIndex.createIndexName`.
   *
   * An unmatched `}` in the pattern surfaces as `InvalidCustomIndexNameException` (a bare
   * `RuntimeException`), unlike every OTHER construction-time rejection in `kcqlMap`, which is an
   * `IllegalArgumentException` — rewrapped here so all of them present one exception contract.
   */
  private def resolveIndexSuffix(kcql: Kcql, target: String): Option[CustomIndexName] =
    Option(kcql.getIndexSuffix).map { raw =>
      try {
        val parsed = CustomIndexName.parseIndexName(raw)
        val _      = parsed.toString
        parsed
      } catch {
        case e: InvalidCustomIndexNameException =>
          throw new IllegalArgumentException(
            s"KCQL targeting '$target' has an invalid WITHINDEXSUFFIX pattern '$raw': ${e.getMessage}",
            e,
          )
      }
    }

  // Eagerly invoke the client's start lifecycle hook so any cluster-compatibility
  // failures (e.g. pointing at an Elasticsearch cluster) are surfaced at SinkTask.start,
  // not on the first batch of records. Runs AFTER kcqlMap above so a bad KCQL never reaches the
  // cluster at all.
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

  // Auto-create indexes. Deduplicate by resolved index name so each index is created at most
  // once. Reuses kcqlMap's already-resolved targetType/indexSuffix rather than re-parsing both
  // from the raw Kcql a second time (CreateIndex.getIndexNameForAutoCreate(Kcql) would otherwise
  // re-run Kcql.getTargetType() and CustomIndexName.parseIndexName).
  settings.kcqls
    .filter(_.isAutoCreate)
    .flatMap { kcql =>
      val kcqlValue = kcqlMap.get(kcql)
      CreateIndex.getIndexNameForAutoCreate(kcqlValue.targetType, kcqlValue.indexSuffix).fold(throw _, Some(_))
    }
    .distinct
    .foreach { indexName =>
      client.createIndex(indexName).fold(throw _, identity)
    }

  def close(): Unit = client.close()

  /**
   * Writes a batch of records. Returns the subset diverted (dropped or DLQ'd) rather than
   * written, identified by [[RecordRef]] — callers should exclude these from delivered-record
   * accounting.
   *
   * Dedup tracking is scoped to this call: a fresh tracker per `write()`, so diversions are not
   * suppressed across a redelivered `put()` (at-least-once per whole-`put()` redelivery; see
   * [[DlqKey]] and [[handleBulkResult]] for the mixed-error deferral this interacts with).
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
      // Writer-stage failures (record, target, cause) collected across the WHOLE write() call —
      // every topic/KCQL/sub-batch — and diverted in exactly ONE divertWriterStage call below,
      // after every bulk call in this put() has already succeeded. See
      // See ErrantRecordHandler.divertWriterStage's scaladoc for why per-record or per-sub-batch
      // diversion would be unsafe (DLQ deadline amplification / duplicate reports on redelivery).
      val pendingWriterStage = mutable.Buffer.empty[WriterStageFailure]
      insert(grouped, reportedThisPut, pendingWriterStage)

      if (pendingWriterStage.nonEmpty) {
        errantRecordHandler.divertWriterStage(pendingWriterStage.toSeq, effectiveMode, reportedThisPut) match {
          case DivertOutcome.Diverted    => ()
          case DivertOutcome.NotDiverted =>
            // Picked deterministically by (topic, partition, offset) — NOT "the first one Try
            // happened to fail on" — so this exception's cause/message is reproducible for the
            // same input regardless of Map/HashMap iteration order over topics/sub-batches.
            val representativeCause =
              pendingWriterStage.minBy(f => (f.record.topic(), f.record.kafkaPartition(), f.record.kafkaOffset())).cause
            throw new WriterStageDiversionFailedException(
              s"Failed to divert ${pendingWriterStage.size} writer-stage failure(s) under " +
                s"bulk.item.errors=${effectiveMode.configValue} (DLQ ack timeout, report() throwing " +
                s"under errors.tolerance=none, or no DLQ available); representative underlying " +
                s"cause (lowest topic-partition-offset among ${pendingWriterStage.size} failure(s)): " +
                s"${representativeCause.getMessage}",
              representativeCause,
            )
        }
      }

      if (reportedThisPut.nonEmpty) {
        logger.info(
          s"Diverted ${reportedThisPut.size} record(s) (bulk.item.errors=${effectiveMode.configValue}) during this put().",
        )
      }
      reportedThisPut.keys.map(k => RecordRef(k.topic, k.partition, k.offset)).toSet
    }

  private def insert(
    records:            Map[String, Vector[SinkRecord]],
    reportedThisPut:    mutable.Map[DlqKey, SinkRecord],
    pendingWriterStage: mutable.Buffer[WriterStageFailure],
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
          val target = Option(kcql.getTarget).getOrElse(topic)
          sinkRecords.grouped(settings.batchSize).foreach { batch =>
            val attempted: Vector[Either[WriterStageFailure, Option[(BulkOp, SinkRecord)]]] =
              batch.map { r =>
                Try(processRecord(topic, kcql, kcqlValue, r)) match {
                  case Success(opt) => Right(opt.map(_ -> r))
                  // Any writer-stage failure is divertible whenever diversion is active — every
                  // KCQL/config-level fault that would fail identically for every record on the
                  // topic (a nested header PK path, UPSERT with no PK, an unsupported write
                  // mode, an unparseable KCQL target or WITHINDEXSUFFIX pattern) is validated at
                  // construction instead (see kcqlMap above), so it never reaches this Try at
                  // all — it fails the task at start(), in every mode, before any record is
                  // written. scala.util.Try only ever catches NonFatal, so a VM error or
                  // InterruptedException still propagates through untouched, in every mode.
                  case Failure(e) if divertActive => Left(WriterStageFailure(r, target, e))
                  case Failure(e)                 => throw e
                }
              }
            pendingWriterStage ++= attempted.collect { case Left(f) => f }
            val opsWithRecords: Vector[(BulkOp, SinkRecord)] =
              attempted.collect { case Right(Some(pair)) => pair }
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
   *    batch-wide 400s). A batch-wide failure blocks diversion for the whole batch: throws a
   *    plain [[BulkItemErrorClassifier.exceptionFor]] (not retriable — redelivery can't clear a
   *    batch-wide condition).
   *  - A retriable (429/409) remainder in the SAME response defers diversion entirely, even if
   *    every other error is divertible: nothing is diverted, full error set thrown via
   *    [[BulkItemErrorClassifier.deferredDiversionExceptionFor]] — a
   *    [[io.lenses.streamreactor.common.errors.RetriableIntegrityException]] — for redelivery.
   *    Diverting the permanent subset immediately and throwing only for the retriable remainder
   *    would re-divert the same record(s) on every redelivery while the retriable condition
   *    persists; deferring diverts a mixed batch at most once, on whichever attempt comes back
   *    with only divertible errors. Being retriable matters under `error.policy=RETRY`:
   *    [[io.lenses.streamreactor.common.errors.RetryErrorPolicy]] never wraps a plain
   *    `FatalConnectException` in `RetriableException`, so without this the task would die on
   *    the first mixed response. Under `THROW`/`NOOP` both already fail fast regardless, so only
   *    redelivery-on-restart (not an in-process retry) can retry the batch.
   *  - `errantRecordHandler.divertPermanent` returning
   *    [[io.lenses.streamreactor.connect.elastic.common.dlq.DivertOutcome.NotDiverted]] (untrusted
   *    position mapping, DLQ ack timeout) throws a plain [[BulkItemErrorClassifier.exceptionFor]]
   *    too — unlike the deferred case, a clean redelivery isn't guaranteed to change the outcome.
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
      if (!divertActive || perDocumentRejections.isEmpty || batchWideFailures.nonEmpty) {
        // Diversion is inactive, nothing is divertible, or a batch-wide error is present: throw
        // for the full error set (pre-diversion behaviour). Never retriable purely because of
        // this batch's shape — see scaladoc above.
        Failure(BulkItemErrorClassifier.exceptionFor(result.itemErrors))
      } else if (retriable.nonEmpty) {
        // Every non-retriable error here is divertible (the branches above ruled out "nothing
        // divertible" and "batch-wide"); diversion is deferred to a clean attempt, and that
        // attempt is guaranteed to make progress once the retriable remainder clears — see
        // scaladoc above.
        Failure(BulkItemErrorClassifier.deferredDiversionExceptionFor(result.itemErrors))
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
   * Classifies a transport-level failure (no per-item response received at all) as
   * [[RetriableIntegrityException]] — not swallowed by NOOP, retried under `error.policy=RETRY`,
   * never diverted (no per-item data to attribute it to). Safe to retry: INSERT/UPSERT are keyed
   * by document `id` (re-sending overwrites identically) and DELETE is naturally idempotent.
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
    val i            = CreateIndex.getIndexName(kcqlValue.targetType, kcqlValue.indexSuffix, r).leftMap(throw _).merge
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
            // pks.nonEmpty is guaranteed here: UPSERT with an empty primaryKeysPath is rejected
            // at construction (kcqlMap above), before any record is written.
            Some(UpsertOp(index = i, id = idFromPk, json = value, documentType = documentType))

          case other =>
            // Unreachable: write mode is validated to be INSERT/UPSERT at construction, before
            // any record is written (see the "Validate write modes upfront" block above) — this
            // branch exists only as defensive dead code. Note this is NOT one of the cases moved
            // to construction-time validation for the sake of divertibility: it's a NonFatal
            // Throwable like any other, so IF it were ever somehow reached, insert's Try would
            // treat it the same as any other writer-stage failure and divert it under
            // bulk.item.errors=drop/dlq — acceptable only because it is provably unreachable.
            throw new IllegalStateException(
              s"Unsupported KCQL write mode: $other; only INSERT and UPSERT are supported. This should be " +
                "unreachable — write mode is validated at construction.",
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
        // Any writer-stage failure is divertible now (see insert's Try above), tombstone-ness
        // included; TombstoneRejectedException carries no special marker any more, but keeps its
        // own type (rather than a bare IllegalStateException) so callers/tests distinguishing it
        // from other failures under bulk.item.errors=fail are unaffected.
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

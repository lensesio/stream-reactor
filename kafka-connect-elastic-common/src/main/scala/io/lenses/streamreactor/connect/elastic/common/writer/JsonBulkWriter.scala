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
import io.lenses.streamreactor.connect.elastic.common.KcqlValues
import io.lenses.streamreactor.connect.elastic.common.NullValueBehavior
import io.lenses.streamreactor.connect.elastic.common.Transform
import io.lenses.streamreactor.connect.elastic.common.TransformAndExtractPK
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemErrorClassifier
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.indexname.CreateIndex
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import scala.util.Failure
import scala.util.Success
import scala.jdk.CollectionConverters._

class JsonBulkWriter(client: KBulkClient, val settings: ElasticCommonSettings) extends ErrorHandler with StrictLogging {

  logger.info("Initialising Json bulk writer")

  initialize(settings.taskRetries, settings.errorPolicy)

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

  def write(records: Vector[SinkRecord]): Unit =
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }

  private def insert(records: Map[String, Vector[SinkRecord]]): Unit = {
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
            val ops: Vector[BulkOp] = batch.flatMap { r =>
              processRecord(topic, kcql, kcqlValue, r)
            }
            if (ops.nonEmpty) {
              val bulkTry = client.bulk(ops).flatMap { result =>
                if (result.errors) {
                  // Permanent vs retriable item errors are classified here. Per-record DLQ via
                  // ErrantRecordReporter is not implemented yet (lensesio-dev/stream-reactor#414).
                  Failure(BulkItemErrorClassifier.exceptionFor(result.itemErrors))
                } else {
                  Success(result)
                }
              }
              handleTry(bulkTry)
            }
          }
        }
    }
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

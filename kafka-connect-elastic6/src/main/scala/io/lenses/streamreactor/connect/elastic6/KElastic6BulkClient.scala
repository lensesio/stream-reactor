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
package io.lenses.streamreactor.connect.elastic6

import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.http.ElasticDsl
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemErrorClassifier
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * Adapts the elastic4s-6-based [[KElasticClient]] to the [[KBulkClient]] trait.
 *
 * Elastic 6 requires a document type in index/update/delete operations.
 * The fallback (per original [[ElasticJsonWriter]] behaviour) is to use the index name
 * as the document type when the KCQL `WITHDOCTYPE` clause is absent.
 *
 * HTTP-transport errors are surfaced via the returned `Try`. Per-item bulk errors are controlled
 * by [[strictItemErrors]] (config `connect.elastic.bulk.strict.item.errors`, default true):
 *  - true: `BulkResult.errors=true` so JsonBulkWriter classifies 429 vs mapper errors
 *  - false: logged at WARN and dropped (legacy tolerant mode)
 *
 * @param writeTimeoutMillis timeout in **milliseconds** for `Await.result` on the bulk Future.
 *                           The same value is applied as the HTTP connect/socket timeout.
 *                           Default 300000 = 5 minutes.
 */
class KElastic6BulkClient(
  client:             KElasticClient,
  writeTimeoutMillis: Int,
  strictItemErrors:   Boolean = true,
) extends KBulkClient
    with StrictLogging {

  override def supportsDocumentType: Boolean = true

  if (writeTimeoutMillis < 1000) {
    logger.warn(
      s"connect.elastic.write.timeout=$writeTimeoutMillis is less than 1 second. This setting is in milliseconds.",
    )
  }

  override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = Try {
    import ElasticDsl._

    val elasticRequests = ops.map {
      case InsertOp(index, id, json, pipeline, documentType) =>
        val docType = documentType.getOrElse(index)
        indexInto(index / docType)
          .id(id)
          .pipeline(pipeline.orNull)
          .source(json.toString)

      case UpsertOp(index, id, json, documentType) =>
        val docType = documentType.getOrElse(index)
        update(id)
          .in(index / docType)
          .docAsUpsert(json.toString)
          .retryOnConflict(ElasticCommonConfigConstants.UPSERT_RETRY_ON_CONFLICT)

      case DeleteOp(index, id, documentType) =>
        val docType = documentType.getOrElse(index)
        deleteById(new Index(index), docType, id)
    }

    val response = Await.result(client.execute(ElasticDsl.bulk(elasticRequests)), writeTimeoutMillis.millis)

    if (response.isError) {
      throw new RuntimeException(s"Elastic bulk transport error: ${response.error.reason}")
    }

    val result     = response.result
    val tookMillis = result.took

    val itemErrors: Seq[BulkItemError] = result.items.collect {
      case item if item.error.isDefined =>
        val err = item.error.get
        BulkItemError(
          index     = item.index,
          id        = item.id,
          reason    = err.reason,
          errorType = err.`type`,
          status    = item.status,
        )
    }

    if (itemErrors.nonEmpty) {
      if (strictItemErrors) {
        logger.error(s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(itemErrors)}")
      } else {
        logger.warn(
          s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(itemErrors)} (tolerant mode)",
        )
      }
    }

    logger.info(s"Bulk write completed: took=${tookMillis}ms, items=${result.items.size}")
    if (itemErrors.nonEmpty && strictItemErrors) {
      BulkResult(took = tookMillis, errors = true, itemErrors = itemErrors)
    } else {
      BulkResult(took = tookMillis, errors = false, itemErrors = Seq.empty)
    }
  }

  override def createIndex(name: String): Try[Unit] = Try {
    client.createIndex(name)
  }

  override def close(): Unit = client.close()
}

object KElastic6BulkClient {
  def apply(client: KElasticClient, settings: ElasticCommonSettings): KElastic6BulkClient =
    new KElastic6BulkClient(client, settings.writeTimeout, settings.strictItemErrors)
}

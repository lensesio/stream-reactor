/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer

import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkTaskContext
import com.azure.cosmos.CosmosClient
import cats.syntax.either._
import scala.util.Try
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter.SinkRecordToDocument
import org.apache.kafka.connect.sink.SinkRecord
import io.lenses.streamreactor.connect.azure.cosmosdb.config.KeySource
import com.azure.cosmos.implementation.Document

//Factory to build
object CosmosDbWriterManagerFactory extends StrictLogging {
  def apply(
    configMap:      Map[String, Kcql],
    settings:       CosmosDbSinkSettings,
    context:        SinkTaskContext,
    documentClient: CosmosClient,
    fnConvertToDocument: (SinkRecord, Map[String, String], Set[String], KeySource) => Either[Throwable, Document] =
      SinkRecordToDocument.apply,
  ): Either[Throwable, CosmosDbWriterManager] =
    for {
      sinkName <- settings.sinkName.asRight[Throwable]
      _ <- {
        settings.errorPolicy match {
          case RetryErrorPolicy() =>
            context.timeout(settings.taskRetryInterval)
          case _ =>
        }
        ().asRight[Throwable]
      }
      _ = logger.info(s"Initialising Document Db writer.")
      _ <- configMap.foldLeft(Right(()): Either[Throwable, Unit]) { (acc, entry) =>
        acc.flatMap { _ =>
          val (_, c) = entry
          Try {
            Option(documentClient.getDatabase(settings.database).getContainer(c.getTarget))
              .getOrElse(throw new ConnectException(s"Collection [${c.getTarget}] not found!"))
          }.toEither.map(_ => ())
        }
      }
      batchPolicyMap <- settings.kcql
        .map(c => c.getSource -> settings.commitPolicy(c))
        .toMap
        .asRight[Throwable]
      curriedFn = (rec: SinkRecord, fields: Map[String, String], ignored: Set[String]) =>
        fnConvertToDocument(rec, fields, ignored, settings.keySource)
      writerManager <-
        Try(
          new CosmosDbWriterManager(sinkName, configMap, batchPolicyMap, settings, documentClient, curriedFn),
        ).toEither
    } yield writerManager
}

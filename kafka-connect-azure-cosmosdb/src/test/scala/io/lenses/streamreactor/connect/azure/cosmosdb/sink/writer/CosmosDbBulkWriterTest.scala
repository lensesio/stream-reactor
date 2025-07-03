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

import com.azure.cosmos.implementation.Document
import com.azure.cosmos.models.CosmosBulkOperations
import com.azure.cosmos.models.CosmosItemRequestOptions
import com.azure.cosmos.models.PartitionKey
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.CosmosDbTestUtils
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Answers
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CosmosDbBulkWriterTest
    extends AnyFunSuite
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues
    with CosmosDbTestUtils {

  private def mockSettings: CosmosDbSinkSettings = {
    val settings = mock[CosmosDbSinkSettings](Answers.RETURNS_DEEP_STUBS)
    when(settings.consistency).thenReturn(com.azure.cosmos.ConsistencyLevel.SESSION)
    when(settings.fields(any[String])).thenReturn(Map.empty)
    when(settings.ignoredField(any[String])).thenReturn(Set.empty)
    settings
  }

  private def withWriter(
    conversions:         Seq[(SinkRecord, Either[Throwable, PendingRecord])],
    unrecoverable:       Option[Throwable],
    fnConvertToDocument: (SinkRecord, Map[String, String], Set[String]) => Either[Throwable, Document],
  )(test: (CosmosDbBulkWriter, CosmosRecordsQueue[PendingRecord], CosmosDbQueueProcessor, Kcql, Seq[SinkRecord],
      Seq[PendingRecord]) => Unit,
  ): Unit = {

    val mockQueue = mock[CosmosRecordsQueue[PendingRecord]]

    val mockProcessor = mock[CosmosDbQueueProcessor]
    when(mockProcessor.unrecoverableError()).thenReturn(unrecoverable)

    val mockKcql: Kcql = mockKcqlObject

    val records = conversions.toMap.keys.toSeq
    records.foreach(r => when(r.topic()).thenReturn("test-topic"))

    val pendingRecords = conversions.collect { case (_, Right(pending)) => pending }

    val writer = new CosmosDbBulkWriter(mockKcql, mockQueue, mockProcessor, mockSettings, fnConvertToDocument)
    test(writer, mockQueue, mockProcessor, mockKcql, records, pendingRecords)
  }

  private def mockKcqlObject = {
    val mockKcql = mock[Kcql]
    when(mockKcql.getWriteMode).thenReturn(WriteModeEnum.INSERT)
    mockKcql
  }

  private def dummyPendingRecord: PendingRecord = {
    val topic = Topic("test-topic")
    val tpo   = TopicPartitionOffset(topic, 0, Offset(0L))
    val doc   = mockDocument()
    val op    = CosmosBulkOperations.getCreateItemOperation(doc, new PartitionKey("dummy"), new CosmosItemRequestOptions())
    PendingRecord(tpo, doc, op)
  }

  private def mockDocument(id: String = "some-id"): Document = {
    val doc = mock[Document]
    when(doc.getId).thenReturn(id)
    doc
  }

  test("insert should enqueue converted records when no errors and no unrecoverable error") {
    val record = mock[SinkRecord]
    val doc    = mockDocument()
    val pending = {
      val topic = Topic("test-topic")
      val tpo   = TopicPartitionOffset(topic, 0, Offset(0L))
      val op =
        CosmosBulkOperations.getCreateItemOperation(doc, new PartitionKey("dummy"), new CosmosItemRequestOptions())
      PendingRecord(tpo, doc, op)
    }
    withWriter(Seq(record -> Right(pending)), None, (_, _, _) => Right(doc)) {
      (writer, mockQueue, mockProcessor, mockKcql, records, pendingRecords) =>
        val enqueued = pendingRecords.toList
        val result   = writer.insert(records)
        result.isRight shouldBe true
        verify(mockQueue).enqueueAll(
          argThat { (actual: List[PendingRecord]) =>
            actual.size == enqueued.size &&
            actual.zip(enqueued).forall { case (a, b) =>
              a.topicPartitionOffset == b.topicPartitionOffset &&
                a.document.getId == b.document.getId
            }
          },
        )
    }
  }

  test("insert should log and skip errored conversions, but enqueue valid ones") {
    val record1 = mock[SinkRecord]
    val record2 = mock[SinkRecord]
    val error   = new RuntimeException("bad record")
    val doc     = mockDocument()
    val pending = {
      val topic = Topic("test-topic")
      val tpo   = TopicPartitionOffset(topic, 0, Offset(0L))
      val op =
        CosmosBulkOperations.getCreateItemOperation(doc, new PartitionKey("dummy"), new CosmosItemRequestOptions())
      PendingRecord(tpo, doc, op)
    }
    withWriter(Seq(record1 -> Left(error), record2 -> Right(pending)),
               None,
               {
                 case (`record1`, _, _) => Left(error)
                 case _                 => Right(doc)
               },
    ) { (writer, mockQueue, mockProcessor, mockKcql, records, pendingRecords) =>
      val enqueued = pendingRecords.toList
      val result   = writer.insert(records)
      result.isRight shouldBe true
      verify(mockQueue).enqueueAll(
        argThat { (actual: List[PendingRecord]) =>
          actual.size == enqueued.size &&
          actual.zip(enqueued).forall { case (a, b) =>
            a.topicPartitionOffset == b.topicPartitionOffset &&
              a.document.getId == b.document.getId
          }
        },
      )
    }
  }

  test("insert should return Left if unrecoverableError is present") {
    val record  = mock[SinkRecord]
    val error   = new RuntimeException("fatal")
    val pending = dummyPendingRecord
    withWriter(Seq(record -> Right(pending)),
               Some(error),
               (_, _, _) => {
                 val doc = mockDocument()
                 Right(doc)
               },
    ) { (writer, mockQueue, mockProcessor, mockKcql, records, _) =>
      val result = writer.insert(records)
      result.left.value shouldBe error
      verify(mockQueue, never).enqueueAll(any[List[PendingRecord]])
    }
  }

  test("preCommit should delegate to processor") {
    withWriter(Seq(),
               None,
               (_, _, _) => {
                 val doc = mockDocument()
                 Right(doc)
               },
    ) { (writer, _, mockProcessor, mockKcql, _, _) =>
      val offsets  = Map(Topic("topic").withPartition(0) -> new OffsetAndMetadata(123L))
      val expected = Map(Topic("topic").withPartition(0) -> new OffsetAndMetadata(123L))
      when(mockProcessor.preCommit(offsets)).thenReturn(expected)
      writer.preCommit(offsets) shouldBe expected; ()
    }
  }

  test("unrecoverableError should delegate to processor") {
    val error = new RuntimeException("fail")
    withWriter(Seq(),
               Some(error),
               (_, _, _) => {
                 val doc = mockDocument()
                 Right(doc)
               },
    ) { (writer, _, mockProcessor, mockKcql, _, _) =>
      when(mockProcessor.unrecoverableError()).thenReturn(Some(error))
      writer.unrecoverableError() shouldBe Some(error); ()
    }
  }

  test("close should delegate to queueProcessor") {
    withWriter(Seq(),
               None,
               (_, _, _) => {
                 val doc = mockDocument()
                 Right(doc)
               },
    ) { (writer, _, mockProcessor, mockKcql, _, _) =>
      writer.close()
      verify(mockProcessor).close()
    }
  }
}

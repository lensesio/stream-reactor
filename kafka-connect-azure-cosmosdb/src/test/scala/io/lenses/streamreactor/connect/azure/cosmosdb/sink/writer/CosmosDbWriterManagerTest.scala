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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer

import com.azure.cosmos.CosmosClient
import com.azure.cosmos.implementation.Document
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.config.KeyKeySource
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.Answers
import org.mockito.MockitoSugar
import org.scalatest.EitherValues._
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CosmosDbWriterManagerTest extends AnyFunSuite with Matchers with MockitoSugar with OptionValues {

  private def mockSettings(
    bulkEnabled: Boolean                                  = false,
    taskRetries: Int                                      = 3,
    errorPolicy: Option[ErrorPolicy]                      = None,
    delay:       scala.concurrent.duration.FiniteDuration = scala.concurrent.duration.FiniteDuration(1, "second"),
    maxQueueOfferTimeout: scala.concurrent.duration.FiniteDuration = scala.concurrent.duration.FiniteDuration(1,
      "second"),
    maxQueueSize:    Int = 1,
    executorThreads: Int = 1,
    errorThreshold:  Int = 1,
  ): CosmosDbSinkSettings = {
    val settings = mock[CosmosDbSinkSettings](Answers.RETURNS_DEEP_STUBS)
    when(settings.bulkEnabled).thenReturn(bulkEnabled)
    when(settings.taskRetries).thenReturn(taskRetries)
    when(settings.errorPolicy).thenReturn(errorPolicy.getOrElse(mock[ErrorPolicy]))
    when(settings.delay).thenReturn(delay)
    when(settings.maxQueueOfferTimeout).thenReturn(maxQueueOfferTimeout)
    when(settings.maxQueueSize).thenReturn(maxQueueSize)
    when(settings.executorThreads).thenReturn(executorThreads)
    when(settings.errorThreshold).thenReturn(errorThreshold)
    when(settings.fields(any[String])).thenReturn(Map.empty)
    when(settings.ignoredField(any[String])).thenReturn(Set.empty)
    when(settings.keySource).thenReturn(KeyKeySource)
    when(settings.database).thenReturn("dummy-db")
    when(settings.consistency).thenReturn(com.azure.cosmos.ConsistencyLevel.SESSION)
    settings
  }

  private def mockKcql(source: String = "topic"): Kcql = {
    val kcql = mock[Kcql]
    when(kcql.getSource).thenReturn(source)
    when(kcql.getWriteMode).thenReturn(WriteModeEnum.INSERT)
    kcql
  }

  private def mockBatchPolicy: BatchPolicy  = mock[BatchPolicy]
  private def mockClient:      CosmosClient = mock[CosmosClient]
  private def mockSinkRecord(topic: String): SinkRecord = {
    val record = mock[SinkRecord]
    when(record.topic()).thenReturn(topic)
    record
  }
  private def mockWriter(insertResult: Either[Throwable, Unit] = Right(())): CosmosDbWriter = {
    val writer = mock[CosmosDbWriter]
    when(writer.insert(any[Iterable[SinkRecord]])).thenReturn(insertResult)
    writer
  }

  private def mockDocument(id: String = "some-id"): Document = {
    val doc = mock[Document]
    when(doc.getId).thenReturn(id)
    doc
  }

  test("write should not throw when given empty records") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    noException should be thrownBy writerManager.write(Seq.empty[SinkRecord])
  }

  test("close should close client (integration)") {
    val client = mockClient
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      client,
      (_, _, _) => Right(mockDocument()),
    )
    val record = mockSinkRecord("topic")
    writerManager.write(Seq(record))
    noException should be thrownBy writerManager.close()
    verify(client).close()
  }

  test("preCommit should return original offsets if no writers exist") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val tpo     = Topic("topic").withPartition(0).atOffset(123L)
    val offsets = Map(tpo.toTopicPartition -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(tpo.offset.value))
    val result  = writerManager.preCommit(offsets)
    result shouldBe offsets
  }

  test("write should handle error when config is missing for topic") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map.empty,
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val record = mockSinkRecord("notopic")
    noException should be thrownBy writerManager.write(Seq(record))
  }

  test("write should handle error when batch policy is missing for topic in bulk mode") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map.empty,
      mockSettings(bulkEnabled = true),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val record = mockSinkRecord("notopic")
    noException should be thrownBy writerManager.write(Seq(record))
  }

  test("write should create and use bulk writer if not present and bulkEnabled is true") {
    val mockBulkWriter = mock[CosmosDbBulkWriter]
    val writerManager = new TestableWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(bulkEnabled = true),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    ) {
      override private[cosmosdb] def createWriter(recordTopic: Topic) = Right(mockBulkWriter)
    }
    val record = mockSinkRecord("topic")
    writerManager.write(List(record))
    writerManager.writers(Topic("topic")) shouldBe mockBulkWriter
  }

  test("write should use existing writer for topic") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val mockWriterInstance = mockWriter()
    val topic              = Topic("topic")
    writerManager.writers.update(topic, mockWriterInstance)
    val record = mockSinkRecord("topic")
    writerManager.write(Seq(record))
    verify(mockWriterInstance).insert(Seq(record))
  }

  test("write should create and use single writer if not present") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val record = mockSinkRecord("topic")
    noException should be thrownBy writerManager.write(Seq(record))
  }

  test("insert should handle createWriter returning Left (error)") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map(),
      Map(),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val record = mockSinkRecord("notopic")
    noException should be thrownBy writerManager.write(Seq(record))
  }

  test("insert should handle writer.insert returning Left (error)") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val mockWriterInstance = mockWriter(Left(new RuntimeException("fail")))
    val topic              = Topic("topic")
    writerManager.writers.update(topic, mockWriterInstance)
    val record = mockSinkRecord("topic")
    noException should be thrownBy writerManager.write(Seq(record))
  }

  class TestableWriterManager(
    sinkName:       String,
    configMap:      Map[String, Kcql],
    batchPolicyMap: Map[String, BatchPolicy],
    settings:       CosmosDbSinkSettings,
    documentClient: CosmosClient,
    fnConvertToDocument: (org.apache.kafka.connect.sink.SinkRecord, Map[String, String],
      Set[String]) => Either[Throwable, Document] = (_, _, _) => Right(mockDocument()),
  ) extends CosmosDbWriterManager(sinkName, configMap, batchPolicyMap, settings, documentClient, fnConvertToDocument) {
    def testCreateWriter(topic: Topic): Either[ConnectException, CosmosDbWriter] = createWriter(topic)
  }

  test("createWriter should return Left if topic not in configMap") {
    val manager = new TestableWriterManager(
      "sinkName",
      Map.empty,
      Map.empty,
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val result = manager.testCreateWriter(Topic("notopic"))
    result.isLeft shouldBe true
    result.left.value.getMessage should include("not handled by the configuration")
  }

  test("createWriter should return Left if topic not in batchPolicyMap and bulkEnabled is true") {
    val manager = new TestableWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map.empty,
      mockSettings(bulkEnabled = true),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val result = manager.testCreateWriter(Topic("topic"))
    result.isLeft shouldBe true
    result.left.value.getMessage should include("not handled by the configuration")
  }

  test("close should close all writers") {
    val client = mockClient
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      client,
      (_, _, _) => Right(mockDocument()),
    )
    val mockWriterInstance = mockWriter()
    val topic              = Topic("topic")
    writerManager.writers.update(topic, mockWriterInstance)
    writerManager.close()
    verify(mockWriterInstance).close()
    verify(client).close()
  }

  test("preCommit should delegate to writer if present") {
    val writerManager = new CosmosDbWriterManager(
      "sinkName",
      Map("topic" -> mockKcql()),
      Map("topic" -> mockBatchPolicy),
      mockSettings(),
      mockClient,
      (_, _, _) => Right(mockDocument()),
    )
    val mockWriterInstance = mockWriter()
    val topic              = Topic("topic")
    writerManager.writers.update(topic, mockWriterInstance)
    val tpo      = Topic("topic").withPartition(0).atOffset(123L)
    val offsets  = Map(tpo.toTopicPartition -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(tpo.offset.value))
    val expected = Map(tpo.toTopicPartition -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(456L))
    when(mockWriterInstance.preCommit(offsets)).thenReturn(expected)
    val result = writerManager.preCommit(offsets)
    result shouldBe expected
  }
}

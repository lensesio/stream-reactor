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
import com.azure.cosmos.models.CosmosItemRequestOptions
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.CosmosDbTestUtils
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Answers
import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CosmosDbSingleWriterTest
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
    when(settings.database).thenReturn("dummy-db")
    settings
  }

  private def withWriter(
    fnConvertToDocument: (SinkRecord, Map[String, String], Set[String]) => Either[Throwable, Document],
  )(test:                (CosmosDbSingleWriter, CosmosClient) => Unit,
  ): Unit = {
    val mockKcql   = mock[Kcql]
    val mockClient = mock[CosmosClient]
    val settings   = mockSettings
    val writer     = new CosmosDbSingleWriter(mockKcql, settings, mockClient, fnConvertToDocument)
    test(writer, mockClient)
  }

  private def mockDocument(id: String = "some-id"): Document = {
    val doc = mock[Document]
    when(doc.getId).thenReturn(id)
    doc
  }

  test("insert should call createItem for INSERT mode") {
    withWriter((_, _, _) => Right(mockDocument())) { (writer, mockClient) =>
      val record = mock[SinkRecord]
      Mockito.when(record.topic()).thenReturn("test-topic")
      val mockDb        = mock[com.azure.cosmos.CosmosDatabase]
      val mockContainer = mock[com.azure.cosmos.CosmosContainer]
      Mockito.when(mockClient.getDatabase("dummy-db")).thenReturn(mockDb)
      Mockito.when(mockDb.getContainer(any[String])).thenReturn(mockContainer)
      Mockito.when(mockContainer.createItem(any[Document], any[CosmosItemRequestOptions])).thenReturn(null)
      val kcqlField = writer.getClass.getDeclaredField("config")
      kcqlField.setAccessible(true)
      val kcql = kcqlField.get(writer).asInstanceOf[Kcql]
      when(kcql.getWriteMode).thenReturn(WriteModeEnum.INSERT)
      writer.insert(Seq(record)).isRight shouldBe true; ()
    }
  }

  test("insert should call upsertItem for UPSERT mode") {
    withWriter((_, _, _) => Right(mockDocument())) { (writer, mockClient) =>
      val record = mock[SinkRecord]
      Mockito.when(record.topic()).thenReturn("test-topic")
      val mockDb        = mock[com.azure.cosmos.CosmosDatabase]
      val mockContainer = mock[com.azure.cosmos.CosmosContainer]
      Mockito.when(mockClient.getDatabase("dummy-db")).thenReturn(mockDb)
      Mockito.when(mockDb.getContainer(any[String])).thenReturn(mockContainer)
      Mockito.when(mockContainer.upsertItem(any[Document], any[CosmosItemRequestOptions])).thenReturn(null)
      val kcqlField = writer.getClass.getDeclaredField("config")
      kcqlField.setAccessible(true)
      val kcql = kcqlField.get(writer).asInstanceOf[Kcql]
      when(kcql.getWriteMode).thenReturn(WriteModeEnum.UPSERT)
      writer.insert(Seq(record)).isRight shouldBe true; ()
    }
  }

  test("insert should log and skip errored document conversions") {
    withWriter((_, _, _) => Left(new RuntimeException("conversion error"))) { (writer, _) =>
      val record = mock[SinkRecord]
      Mockito.when(record.topic()).thenReturn("test-topic")
      writer.insert(Seq(record)); ()
    }
  }

  test("preCommit should return input and log a warning") {
    val mockKcql   = mock[Kcql]
    val mockClient = mock[CosmosClient]
    val settings   = mockSettings
    val writer     = new CosmosDbSingleWriter(mockKcql, settings, mockClient, (_, _, _) => Right(mockDocument()))
    writer.preCommit(Map.empty) shouldBe Map.empty
  }

  test("unrecoverableError should return None") {
    val mockKcql   = mock[Kcql]
    val mockClient = mock[CosmosClient]
    val settings   = mockSettings
    val writer     = new CosmosDbSingleWriter(mockKcql, settings, mockClient, (_, _, _) => Right(mockDocument()))
    writer.unrecoverableError() shouldBe None
  }

  test("close should be a no-op") {
    val mockKcql   = mock[Kcql]
    val mockClient = mock[CosmosClient]
    val settings   = mockSettings
    val writer     = new CosmosDbSingleWriter(mockKcql, settings, mockClient, (_, _, _) => Right(mockDocument()))
    noException should be thrownBy writer.close()
  }
}

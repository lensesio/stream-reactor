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
import com.azure.cosmos.CosmosContainer
import com.azure.cosmos.CosmosDatabase
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.HttpBatchPolicy
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.connect.errors.ConnectException
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CosmosDbWriterManagerFactoryTest
    extends AnyFunSuite
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues {

  // Helper methods for common mock setups
  private def mockKcqlWithSourceTarget(source: String = "source", target: String = "target"): Kcql = {
    val mockKcql = mock[Kcql]
    when(mockKcql.getSource).thenReturn(source)
    when(mockKcql.getTarget).thenReturn(target)
    mockKcql
  }

  private def mockSettingsWithKcql(
    kcql:           Kcql,
    sinkName:       Either[Throwable, String] = Right("mysink"),
    errorPolicy:    ErrorPolicy               = mock[ErrorPolicy],
    database:       Option[String]            = Some("db"),
    commitPolicyFn: Kcql => BatchPolicy       = _ => HttpBatchPolicy.Default,
  ): CosmosDbSinkSettings = {
    val mockSettings = mock[CosmosDbSinkSettings]
    sinkName match {
      case Right(name) => when(mockSettings.sinkName).thenReturn(name)
      case Left(ex)    => when(mockSettings.sinkName).thenThrow(ex)
    }
    when(mockSettings.errorPolicy).thenReturn(errorPolicy)
    when(mockSettings.kcql).thenReturn(Seq(kcql))
    when(mockSettings.commitPolicy).thenReturn(commitPolicyFn)
    database.foreach(db => when(mockSettings.database).thenReturn(db))
    mockSettings
  }

  private def mockClientWithDatabaseAndContainer(
    dbName:              String                  = "db",
    containerName:       String                  = "target",
    container:           Option[CosmosContainer] = Some(mock[CosmosContainer]),
    throwOnGetContainer: Option[Throwable]       = None,
  ): (CosmosClient, CosmosDatabase) = {
    val mockClient   = mock[CosmosClient]
    val mockDatabase = mock[CosmosDatabase]
    when(mockClient.getDatabase(dbName)).thenReturn(mockDatabase)
    throwOnGetContainer match {
      case Some(ex) => when(mockDatabase.getContainer(containerName)).thenThrow(ex)
      case None     => container.foreach(c => when(mockDatabase.getContainer(containerName)).thenReturn(c))
    }
    (mockClient, mockDatabase)
  }

  private def mockSinkTaskContext: org.apache.kafka.connect.sink.SinkTaskContext =
    mock[org.apache.kafka.connect.sink.SinkTaskContext]

  test("should create CosmosDbWriterManager when all dependencies are valid") {
    val mockKcql       = mockKcqlWithSourceTarget()
    val batchPolicy    = mock[BatchPolicy]
    val commitPolicyFn = mock[Kcql => BatchPolicy]
    when(commitPolicyFn.apply(any[Kcql])).thenReturn(batchPolicy)
    val mockSettings = mockSettingsWithKcql(mockKcql, commitPolicyFn = commitPolicyFn)

    val (mockClient, _) = mockClientWithDatabaseAndContainer()
    val mockContext     = mockSinkTaskContext
    val result = CosmosDbWriterManagerFactory(
      Map("source" -> mockKcql),
      mockSettings,
      mockContext,
      mockClient,
    )
    result.value shouldBe a[CosmosDbWriterManager]
  }

  test("should fail if a collection is missing") {
    val mockKcql     = mockKcqlWithSourceTarget()
    val mockSettings = mockSettingsWithKcql(mockKcql)
    val (mockClient, mockDatabase) =
      mockClientWithDatabaseAndContainer(throwOnGetContainer = Some(new ConnectException("Collection not found!")))
    val mockContext = mockSinkTaskContext
    val result = CosmosDbWriterManagerFactory(
      Map("source" -> mockKcql),
      mockSettings,
      mockContext,
      mockClient,
    )
    val ex = result.left.value
    ex shouldBe a[ConnectException]
    ex.getMessage should include("Collection not found!")
  }

  test("should fail if CosmosDbWriterManager constructor throws") {
    val mockKcql                   = mockKcqlWithSourceTarget()
    val mockSettings               = mockSettingsWithKcql(mockKcql)
    val (mockClient, mockDatabase) = mockClientWithDatabaseAndContainer()
    when(mockDatabase.getContainer("target")).thenThrow(new RuntimeException("WriterManager failed!"))
    val mockContext = mockSinkTaskContext
    val result = CosmosDbWriterManagerFactory(
      Map("source" -> mockKcql),
      mockSettings,
      mockContext,
      mockClient,
    )
    val ex = result.left.value
    ex shouldBe a[RuntimeException]
    ex.getMessage should include("WriterManager failed!")
  }
}

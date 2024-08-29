/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic7

import com.sksamuel.elastic4s.requests.bulk.BulkRequest
import com.sksamuel.elastic4s.requests.delete.DeleteByIdRequest
import com.sksamuel.elastic4s.requests.indexes.IndexRequest
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.kcql.targettype.StaticTargetType
import io.lenses.kcql.targettype.TargetType
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfigConstants.BEHAVIOR_ON_NULL_VALUES_PROPERTY
import io.lenses.streamreactor.connect.elastic7.config.ElasticSettings
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.argThat
import org.mockito.MockitoSugar

import java.util.Collections.emptyList
import java.util.Collections.emptyMap

class ElasticJsonWriterTest extends TestBase with MockitoSugar {

  "ElasticJsonWriter should be able to close" in {
    val KElasticClient  = mock[KElasticClient]
    val elasticSettings = mock[ElasticSettings]
    val kcqls           = Seq.empty

    when(elasticSettings.kcqls).thenReturn(kcqls)

    val target = new ElasticJsonWriter(KElasticClient, elasticSettings)
    target.close()
  }

  "ElasticJsonWriter should be able to write" in {

    val kElasticClient = mock[KElasticClient]

    val sourceTopic = "SOURCE"
    val targetShard = cyclops.control.Either.right[IllegalArgumentException, TargetType](new StaticTargetType("SHARD"))
    val kcql        = mock[Kcql]
    when(kcql.getSource).thenReturn(sourceTopic)
    when(kcql.getTargetType).thenReturn(targetShard)
    when(kcql.getProperties).thenReturn(emptyMap)
    when(kcql.getWriteMode).thenReturn(WriteModeEnum.INSERT)
    when(kcql.getFields).thenReturn(emptyList())
    when(kcql.getIgnoredFields).thenReturn(emptyList())

    val recordKey   = "KEY"
    val recordValue = "{\"id\":\"1\"}"
    val sinkRecord  = mock[SinkRecord]
    when(sinkRecord.topic()).thenReturn(sourceTopic)
    when(sinkRecord.key()).thenReturn(recordKey)
    when(sinkRecord.value()).thenReturn(recordValue)
    when(sinkRecord.valueSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA)

    val elasticSettings = new ElasticSettings(kcqls = Seq(kcql), errorPolicy = new NoopErrorPolicy)

    val target = new ElasticJsonWriter(kElasticClient, elasticSettings)

    target.write(Vector(sinkRecord))

    verify(kElasticClient).execute(argThat { br: BulkRequest =>
      1 == br.requests.size && br.requests(0).isInstanceOf[IndexRequest]
    })
  }

  "ElasticJsonWriter should IGNORE tombstone if no NullValueBehavior specified" in {

    val kElasticClient = mock[KElasticClient]

    val sourceTopic = "SOURCE"
    val targetShard = "SHARD"
    val kcql        = Kcql.parse(s"INSERT INTO $targetShard SELECT * FROM $sourceTopic ")

    val recordKey = "KEY"
    val tombstoneValue: Null = null
    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.topic()).thenReturn(sourceTopic)
    when(sinkRecord.key()).thenReturn(recordKey)
    when(sinkRecord.value()).thenReturn(tombstoneValue)
    when(sinkRecord.valueSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA)

    val elasticSettings = new ElasticSettings(kcqls = Seq(kcql), errorPolicy = new NoopErrorPolicy)

    val target = new ElasticJsonWriter(kElasticClient, elasticSettings)

    target.write(Vector(sinkRecord))

    verify(kElasticClient, times(0)).execute(any[BulkRequest]())
  }

  "ElasticJsonWriter should delete shard on tombstone if no NullValueBehavior is DELETE" in {

    val kElasticClient = mock[KElasticClient]

    val sourceTopic = "SOURCE"
    val targetShard = "SHARD"
    val kcql = Kcql.parse(
      s"INSERT INTO $targetShard SELECT * FROM $sourceTopic " +
        s"PROPERTIES ('$BEHAVIOR_ON_NULL_VALUES_PROPERTY'='${NullValueBehavior.DELETE.toString}')",
    )

    val recordKey = "KEY"
    val tombstoneValue: Null = null
    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.topic()).thenReturn(sourceTopic)
    when(sinkRecord.key()).thenReturn(recordKey)
    when(sinkRecord.value()).thenReturn(tombstoneValue)
    when(sinkRecord.valueSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA)

    val elasticSettings = new ElasticSettings(kcqls = Seq(kcql), errorPolicy = new NoopErrorPolicy)

    val target = new ElasticJsonWriter(kElasticClient, elasticSettings)

    target.write(Vector(sinkRecord))

    verify(kElasticClient).execute(argThat { br: BulkRequest =>
      1 == br.requests.size && br.requests(0).isInstanceOf[DeleteByIdRequest]
    })
  }

  "ElasticJsonWriter should delete shard on tombstone if no NullValueBehavior is FAIL" in {

    val kElasticClient = mock[KElasticClient]

    val sourceTopic = "SOURCE"
    val targetShard = "SHARD"
    val kcql = Kcql.parse(
      s"INSERT INTO $targetShard SELECT * FROM $sourceTopic " +
        s"PROPERTIES ('$BEHAVIOR_ON_NULL_VALUES_PROPERTY'='${NullValueBehavior.FAIL.toString}')",
    )

    val recordKey = "KEY"
    val tombstoneValue: Null = null
    val sinkRecord = mock[SinkRecord]
    when(sinkRecord.topic()).thenReturn(sourceTopic)
    when(sinkRecord.key()).thenReturn(recordKey)
    when(sinkRecord.value()).thenReturn(tombstoneValue)
    when(sinkRecord.valueSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA)

    val elasticSettings = new ElasticSettings(kcqls = Seq(kcql), errorPolicy = new NoopErrorPolicy)

    val target = new ElasticJsonWriter(kElasticClient, elasticSettings)

    val exception = intercept[Exception](target.write(Vector(sinkRecord)))

    exception shouldBe a[IllegalStateException]
  }

}

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
package io.lenses.streamreactor.connect.mqtt.sink

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.mqtt.config.MqttSinkSettings
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.eclipse.paho.client.mqttv3.IMqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.mockito.ArgumentMatchers.{ eq => eqTo }
import org.mockito.ArgumentCaptor
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{ HashMap => JHashMap }
import scala.jdk.CollectionConverters._

class MqttWriterTest extends AnyFlatSpec with Matchers with MockitoSugar with BeforeAndAfter {

  private var client:   IMqttClient      = _
  private var settings: MqttSinkSettings = _

  before {
    client   = mock[IMqttClient]
    settings = mock[MqttSinkSettings]
    when(settings.mqttQualityOfService).thenReturn(1)
    when(settings.mqttRetainedMessage).thenReturn(false)
    when(settings.maxRetries).thenReturn(1)
  }

  "MqttWriter" should "handle string values with static target" in {
    val kcql = Set(Kcql.parse("INSERT INTO mqtt-target SELECT * FROM topic-static"))
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val record = new SinkRecord("topic-static", 0, null, null, null, "test-value", 0)
    writer.write(Seq(record))

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    verify(client).publish(eqTo("mqtt-target"), messageCaptor.capture())
    new String(messageCaptor.getValue.getPayload) shouldBe "test-value"
  }

  it should "use record key as target" in {
    val kcql = Set(Kcql.parse("INSERT INTO _key SELECT * FROM topic-key"))
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val record = new SinkRecord("topic-key", 0, null, "key-target", null, "value", 0)
    writer.write(Seq(record))

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    verify(client).publish(eqTo("key-target"), messageCaptor.capture())
    new String(messageCaptor.getValue.getPayload) shouldBe "value"
  }

  it should "extract target from top-level field in Struct" in {
    val targetPath = "mqtt.target"
    val kcql = Set(
      Kcql.parse(s"INSERT INTO $targetPath SELECT * FROM topic-dynamic PROPERTIES('mqtt.target.from.field'='true')"),
    )
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val schema = SchemaBuilder.struct()
      .field("mqtt",
             SchemaBuilder.struct()
               .field("target", Schema.STRING_SCHEMA)
               .build(),
      )
      .field("value", Schema.STRING_SCHEMA)
      .build()

    val mqtt = new Struct(schema.field("mqtt").schema())
    mqtt.put("target", "target-1")

    val struct = new Struct(schema)
    struct.put("mqtt", mqtt)
    struct.put("value", "test")

    val record = new SinkRecord("topic-dynamic", 0, null, null, schema, struct, 0)
    writer.write(Seq(record))

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    verify(client).publish(eqTo("target-1"), messageCaptor.capture())
    new String(messageCaptor.getValue.getPayload) shouldBe """{"mqtt":{"target":"target-1"},"value":"test"}"""
  }

  it should "extract target from deeply nested field in Struct" in {
    val targetPath = "meta.routing.target"
    val kcql =
      Set(Kcql.parse(s"INSERT INTO $targetPath SELECT * FROM topic-nested PROPERTIES('mqtt.target.from.field'='true')"))
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val routingSchema = SchemaBuilder.struct()
      .field("target", Schema.STRING_SCHEMA)
      .build()

    val metaSchema = SchemaBuilder.struct()
      .field("routing", routingSchema)
      .build()

    val schema = SchemaBuilder.struct()
      .field("meta", metaSchema)
      .field("value", Schema.STRING_SCHEMA)
      .build()

    val routing = new Struct(routingSchema)
    routing.put("target", "nested-target")

    val meta = new Struct(metaSchema)
    meta.put("routing", routing)

    val struct = new Struct(schema)
    struct.put("meta", meta)
    struct.put("value", "test")

    val record = new SinkRecord("topic-nested", 0, null, null, schema, struct, 0)
    writer.write(Seq(record))

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    verify(client).publish(eqTo("nested-target"), messageCaptor.capture())
    new String(
      messageCaptor.getValue.getPayload,
    ) shouldBe """{"meta":{"routing":{"target":"nested-target"}},"value":"test"}"""
  }

  it should "extract target from top-level field in Map" in {
    val targetPath = "mqtt.target"
    val kcql = Set(
      Kcql.parse(s"INSERT INTO $targetPath SELECT * FROM topic-dynamic PROPERTIES('mqtt.target.from.field'='true')"),
    )
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val mqtt = new JHashMap[String, Any]()
    mqtt.put("target", "target-1")

    val map = new JHashMap[String, Any]()
    map.put("mqtt", mqtt)
    map.put("value", "test")

    val record = new SinkRecord("topic-dynamic", 0, null, null, null, map, 0)
    writer.write(Seq(record))

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    verify(client).publish(eqTo("target-1"), messageCaptor.capture())
    new String(messageCaptor.getValue.getPayload) shouldBe """{"mqtt":{"target":"target-1"},"value":"test"}"""
  }

  it should "extract target from deeply nested field in Map" in {
    val targetPath = "meta.routing.target"
    val kcql =
      Set(Kcql.parse(s"INSERT INTO $targetPath SELECT * FROM topic-nested PROPERTIES('mqtt.target.from.field'='true')"))
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val routing = new JHashMap[String, Any]()
    routing.put("target", "nested-target")

    val meta = new JHashMap[String, Any]()
    meta.put("routing", routing)

    val map = new JHashMap[String, Any]()
    map.put("meta", meta)
    map.put("value", "test")

    val record = new SinkRecord("topic-nested", 0, null, null, null, map, 0)
    writer.write(Seq(record))

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    verify(client).publish(eqTo("nested-target"), messageCaptor.capture())
    new String(
      messageCaptor.getValue.getPayload,
    ) shouldBe """{"meta":{"routing":{"target":"nested-target"}},"value":"test"}"""
  }

  it should "route records from different topics to different targets" in {
    val kcql = Set(
      Kcql.parse("INSERT INTO static-target-1 SELECT * FROM topic-1"),
      Kcql.parse("INSERT INTO static-target-2 SELECT * FROM topic-2"),
      Kcql.parse("INSERT INTO meta.target SELECT * FROM topic-3 PROPERTIES('mqtt.target.from.field'='true')"),
    )
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val records = Seq(
      new SinkRecord("topic-1", 0, null, null, null, "value-1", 0),
      new SinkRecord("topic-2", 0, null, null, null, "value-2", 0), {
        val meta = new JHashMap[String, Any]()
        meta.put("target", "dynamic-target")
        val map = new JHashMap[String, Any]()
        map.put("meta", meta)
        map.put("value", "value-3")
        new SinkRecord("topic-3", 0, null, null, null, map, 0)
      },
    )
    writer.write(records)

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    val targetCaptor  = ArgumentCaptor.forClass(classOf[String])
    verify(client, times(3)).publish(targetCaptor.capture(), messageCaptor.capture())

    val targets = targetCaptor.getAllValues.asScala
    targets should contain allOf ("static-target-1", "static-target-2", "dynamic-target")

    val payloads = messageCaptor.getAllValues.asScala.map(msg => new String(msg.getPayload))
    payloads should contain allOf ("value-1", "value-2", """{"meta":{"target":"dynamic-target"},"value":"value-3"}""")
  }

  it should "handle case insensitive property value" in {
    val targetPath = "mqtt.target"
    val kcql = Set(
      Kcql.parse(s"INSERT INTO $targetPath SELECT * FROM topic-dynamic PROPERTIES('mqtt.target.from.field'='TRUE')"),
    )
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)

    val mqtt = new JHashMap[String, Any]()
    mqtt.put("target", "target-1")

    val map = new JHashMap[String, Any]()
    map.put("mqtt", mqtt)
    map.put("value", "test")

    val record = new SinkRecord("topic-dynamic", 0, null, null, null, map, 0)
    writer.write(Seq(record))

    val messageCaptor = ArgumentCaptor.forClass(classOf[MqttMessage])
    verify(client).publish(eqTo("target-1"), messageCaptor.capture())
    new String(messageCaptor.getValue.getPayload) shouldBe """{"mqtt":{"target":"target-1"},"value":"test"}"""
  }

  it should "close client on close" in {
    val kcql = Set(Kcql.parse("INSERT INTO static-target SELECT * FROM topic-static"))
    when(settings.kcql).thenReturn(kcql)
    val writer = new MqttWriter(client, settings)
    writer.close()
    verify(client).disconnect()
    verify(client).close()
  }
}

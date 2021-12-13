/*
 * Copyright 2017 Landoop.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.connect.sql

import java.util

import org.apache.calcite.sql.SqlIdentifier
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.connect.connector.ConnectRecord

/**
  * The SQL transformer. It takes two sql entries for keys and values.
  *
  * @tparam T
  */
class Transformation[T <: ConnectRecord[T]] extends org.apache.kafka.connect.transforms.Transformation[T] {
  private var sqlKeyMap = Map.empty[String, Sql]
  private var sqlValueMap = Map.empty[String, Sql]

  override def apply(record: T): T = {
    val topic = record.topic()

    sqlKeyMap.get(topic).map { sql =>
      val (keySchema, keyValue) = Transform(sql, record.keySchema(), record.key(), true, topic, record.kafkaPartition())
      sqlValueMap.get(topic).map { sql =>
        val (valueSchema, value) = Transform(sql, record.valueSchema(), record.value(), false, topic, record.kafkaPartition())
        record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          keySchema,
          keyValue,
          valueSchema,
          value,
          record.timestamp()
        )
      }.getOrElse {
        record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          keySchema,
          keyValue,
          record.valueSchema(),
          record.value(),
          record.timestamp())
      }
    }.getOrElse {
      sqlValueMap.get(topic).map { sql =>
        val (valueSchema, value) = Transform(sql, record.valueSchema(), record.value(), false, topic, record.kafkaPartition())
        record.newRecord(
          topic,
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          valueSchema,
          value,
          record.timestamp()
        )
      }.getOrElse(record)
    }
  }

  override def config(): ConfigDef = Transformation.configDef

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    val config = new TransformationConfig(configs)

    def fromConfig(query: String) =
      Option(query)
        .map { c =>
          c.split(";")
            .map { k =>
              val sql = Sql.parse(k.trim)
              sql.select.getFrom.asInstanceOf[SqlIdentifier].getSimple -> sql
            }.toMap
        }.getOrElse(Map.empty)

    sqlKeyMap = fromConfig(config.getString(Transformation.KEY_SQL_CONFIG))
    sqlValueMap = fromConfig(config.getString(Transformation.VALUE_SQL_CONFIG))
  }
}


private object Transformation {

  val KEY_SQL_CONFIG = "connect.transforms.sql.key"
  private val KEY_SQL_DOC = "Provides the SQL transformation for the keys. To provide more than one separate them by ';'"
  private val KEY_SQL_DISPLAY = "Key(-s) SQL transformation"

  val VALUE_SQL_CONFIG = "connect.transforms.sql.value"
  private val VALUE_SQL_DOC = "Provides the SQL transformation for the Kafka message value. To provide more than one separate them by ';'"
  private val VALUE_SQL_DISPLAY = "Value(-s) SQL transformation"

  val configDef: ConfigDef = new ConfigDef()
    .define(VALUE_SQL_CONFIG,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.MEDIUM,
      VALUE_SQL_DOC,
      "Transforms",
      1,
      ConfigDef.Width.LONG,
      VALUE_SQL_DISPLAY
    )
    .define(KEY_SQL_CONFIG,
      ConfigDef.Type.STRING,
      null,
      ConfigDef.Importance.MEDIUM,
      KEY_SQL_DOC,
      "Transforms",
      2,
      ConfigDef.Width.LONG,
      KEY_SQL_DISPLAY
    )
}


class TransformationConfig(config: util.Map[String, _]) extends AbstractConfig(Transformation.configDef, config)
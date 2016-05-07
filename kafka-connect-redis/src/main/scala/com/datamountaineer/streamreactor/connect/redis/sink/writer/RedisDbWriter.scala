/**
  * Copyright 2015 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConnectionInfo, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.sink._
import com.google.gson.Gson
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._

/**
  * Responsible for taking a sequence of SinkRecord and write them to Redis
  */
case class RedisDbWriter(connectionInfo: RedisConnectionInfo,
                         fieldsExtractor: StructFieldsExtractor,
                         rowKeyBuilder: StringKeyBuilder) extends DbWriter with StrictLogging {
  val gson = new Gson()
  private val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
  connectionInfo.password.foreach(jedis.auth)


  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.warn("Received empty sequence of SinkRecord")
    }
    else {
      records.foreach { record =>

        logger.debug(s"Received record from topic:${record.topic()} partition:${record.kafkaPartition()} and offset:${record.kafkaOffset()}")
        require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")

        val fieldsAndValues = fieldsExtractor.get(record.value.asInstanceOf[Struct])

        if (fieldsAndValues.nonEmpty) {
          val map = fieldsAndValues.toMap.asJava
          jedis.set(rowKeyBuilder.build(record), gson.toJson(map))
        }
      }
    }
  }

  override def close(): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }
}

object RedisDbWriter {
  def apply(settings: RedisSinkSettings): RedisDbWriter = {

    val keyBuilder = settings.key.mode match {
      case RedisSinkSettings.GenericRowKeyMode => new StringGenericRowKeyBuilder()
      case RedisSinkSettings.SinkRecordRowKeyMode => new SinkRecordKeyStringKeyBuilder()
      case RedisSinkSettings.FieldsRowKeyMode =>
        if (settings.key.fields.isEmpty) {
          throw new ConfigException(s"For ${RedisSinkSettings.FieldsRowKeyMode} the fields creating the key need to be specified.")
        }
        new StructFieldsStringKeyBuilder(settings.key.fields)
      case unkown => throw new ConfigException(s"$unkown is not a recognized key mode")
    }
    new RedisDbWriter(
      settings.connection,
      StructFieldsExtractor(settings.fields.includeAllFields, settings.fields.fieldsMappings),
      keyBuilder)
  }
}

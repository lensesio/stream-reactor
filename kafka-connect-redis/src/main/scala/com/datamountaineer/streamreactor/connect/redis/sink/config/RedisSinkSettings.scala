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

package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import com.datamountaineer.streamreactor.connect.schemas.PayloadFields
import org.apache.kafka.common.config.ConfigException
import scala.util.{Failure, Success, Try}

/**
  * Holds the Redis Sink settings
  */
case class RedisSinkSettings(connection: RedisConnectionInfo,
                             key: RedisKey,
                             fields: PayloadFields)

/**
  * Holds the infomation on how the key is built
  *
  * @param mode Mode for the row key, FIELDS, SINK_RECORD, GENERIC
  * @param fields Fields to write to the target
  */
case class RedisKey(mode: String, fields: Seq[String])


object RedisSinkSettings {

  val FieldsRowKeyMode = "FIELDS"
  val SinkRecordRowKeyMode = "SINK_RECORD"
  val GenericRowKeyMode = "GENERIC"


  /**
    * Creates an instance of RedisSinkSettings from a RedisSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of RedisSinkSettings
    */
  def apply(config: RedisSinkConfig): RedisSinkSettings = {


    val rowKeyMode = config.getString(ROW_KEY_MODE)
    val allRowKeyModes = Set(FieldsRowKeyMode, SinkRecordRowKeyMode, GenericRowKeyMode)
    if (!allRowKeyModes.contains(rowKeyMode)) {
      throw new ConfigException(s"$rowKeyMode is not recognized. Available modes are ${allRowKeyModes.mkString(",")}.")
    }

    RedisSinkSettings(
      RedisConnectionInfo(config),
      RedisKey(rowKeyMode, getRowKeys(rowKeyMode, config)),
      PayloadFields(Try(config.getString(FIELDS)).toOption.flatMap(v => Option(v))))

  }

  /**
    * Returns a sequence of fields making the Redis key.
    *
    * @param rowKeyMode RowKeyMode
    * @param config A Redis Sink configuration for the connector
    * @return A sequence of fields if the row key mode is set to FIELDS, empty otherwise
    */
  private def getRowKeys(rowKeyMode: String, config: RedisSinkConfig): Seq[String] = {
    if (rowKeyMode == FieldsRowKeyMode) {
      val rowKeys = Try(config.getString(ROW_KEYS)) match {
        case Failure(t) => Seq.empty[String]
        case Success(null) => Seq.empty[String]
        case Success(value) => value.split(",").map(_.trim).toSeq
      }
      if (rowKeys.isEmpty) {
        throw new ConfigException("Fields defining the row key are required.")
      }

      rowKeys
    }
    else {
      Seq.empty
    }
  }

}


object RedisConnectionInfo {
  def apply(config: RedisSinkConfig): RedisConnectionInfo = {
    val host = config.getString(REDIS_HOST)
    if (host.isEmpty) {
      throw new ConfigException(s"$REDIS_HOST is not set correctly")
    }

    val passw = Try(config.getString(REDIS_PASSWORD)).toOption

    new RedisConnectionInfo(
      host,
      config.getInt(REDIS_PORT),
      passw)
  }
}

/**
  * Holds the Redis connection details.
  */
case class RedisConnectionInfo(host:String, port:Int, password:Option[String])

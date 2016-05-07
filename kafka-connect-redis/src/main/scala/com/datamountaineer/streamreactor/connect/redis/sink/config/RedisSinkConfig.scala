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

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

import scala.collection.JavaConversions._

object RedisSinkConfig {

  val REDIS_HOST = "connect.redis.connection.host"
  val REDIS_HOST_DOC =
    """
      |Specifies the redis server
    """.stripMargin

  val REDIS_PORT = "connect.redis.connection.port"
  val REDIS_PORT_DOC =
    """
      |Specifies the redis connection port
    """.stripMargin

  val REDIS_PASSWORD = "connect.redis.connection.password"
  val REDIS_PASSWORD_DOC =
    """
      |Provides the password for the redis connection.
    """.stripMargin

  val ROW_KEYS = "connect.redis.sink.keys"
  val ROW_KEYS_DOC =
    """
      |Specifies which of the payload fields make up the Redis key. Multiple fields can be specified by separating them via a comma;
      | The fields are combined using a key separator by default is set to <\\n>.
    """.stripMargin

  val FIELDS = "connect.redis.sink.fields"
  val FIELDS_DOC =
    """
      |Specifies which fields to consider when inserting the new Redis entry. If is not set it will use insert all the payload fields present in the payload.
      |Field mapping is supported; this way an avro record field can be inserted into a 'mapped' column.
      |Examples:
      |* fields to be used:field1,field2,field3
      |** fields with mapping: field1=alias1,field2,field3=alias3"
    """.stripMargin

  val ROW_KEY_MODE = "connect.redis.sink.key.mode"
  val ROW_KEY_MODE_DOC =
    """
      |There are three available modes: SINK_RECORD, GENERIC and FIELDS.
      |SINK_RECORD - uses the SinkRecord.keyValue as the Redis key;
      |FIELDS - combines the specified payload fields to make up the Redis key
      |GENERIC- combines the kafka topic, offset and partition to build the Redis key."
      |
    """.stripMargin

  val config: ConfigDef = new ConfigDef()
    .define(REDIS_HOST, Type.STRING, Importance.HIGH, REDIS_HOST_DOC)
    .define(REDIS_PORT, Type.INT, Importance.HIGH, REDIS_PORT_DOC)
    //.define(REDIS_PASSWORD, Type.PASSWORD, Importance.LOW, REDIS_PASSWORD_DOC)
    .define(ROW_KEY_MODE, Type.STRING, Importance.HIGH, ROW_KEY_MODE_DOC)
    .define(FIELDS, Type.STRING, Importance.LOW, FIELDS_DOC)
    .define(ROW_KEYS, Type.STRING, Importance.LOW, ROW_KEYS_DOC)
}

/**
  * <h1>RedisSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
class RedisSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(RedisSinkConfig.config, props)

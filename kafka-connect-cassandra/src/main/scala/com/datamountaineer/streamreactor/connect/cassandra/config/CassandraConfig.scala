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

package com.datamountaineer.streamreactor.connect.cassandra.config

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/**
  *Holds the base configuration.
  * */
trait CassandraConfig {

    val configDef: ConfigDef = new ConfigDef()
      .define(CassandraConfigConstants.CONTACT_POINTS,
          Type.STRING,
          CassandraConfigConstants.CONTACT_POINT_DEFAULT,
          Importance.HIGH,
          CassandraConfigConstants.CONTACT_POINT_DOC)

      .define(CassandraConfigConstants.KEY_SPACE,
          Type.STRING,
          Importance.HIGH,
          CassandraConfigConstants.KEY_SPACE_DOC)

      .define(CassandraConfigConstants.PORT,
          Type.INT,
          CassandraConfigConstants.PORT_DEFAULT,
          Importance.HIGH,
          CassandraConfigConstants.PORT_DOC)

      .define(CassandraConfigConstants.USERNAME,
          Type.STRING, "",
          Importance.LOW,
          CassandraConfigConstants.USERNAME_DOC)

      .define(CassandraConfigConstants.PASSWD,
          Type.PASSWORD, "",
          Importance.LOW,
          CassandraConfigConstants.PASSWD_DOC)

      .define(CassandraConfigConstants.AUTHENTICATION_MODE,
          Type.STRING,
          CassandraConfigConstants.AUTHENTICATION_MODE_DEFAULT,
          Importance.HIGH,
          CassandraConfigConstants.AUTHENTICATION_MODE_DOC)

      .define(CassandraConfigConstants.SSL_ENABLED,
          Type.BOOLEAN,
          CassandraConfigConstants.SSL_ENABLED_DEFAULT,
          Importance.LOW,
          CassandraConfigConstants.SSL_ENABLED_DOC)

      .define(CassandraConfigConstants.TRUST_STORE_PATH,
          Type.STRING,
          "",
          Importance.LOW,
          CassandraConfigConstants.TRUST_STORE_PATH_DOC)

      .define(CassandraConfigConstants.TRUST_STORE_PASSWD,
          Type.PASSWORD,
          "",
          Importance.LOW,
          CassandraConfigConstants.TRUST_STORE_PASSWD_DOC)

      .define(CassandraConfigConstants.USE_CLIENT_AUTH,
          Type.BOOLEAN,
          CassandraConfigConstants.USE_CLIENT_AUTH_DEFAULT,
          Importance.LOW,
          CassandraConfigConstants.USE_CLIENT_AUTH_DOC)

      .define(CassandraConfigConstants.KEY_STORE_PATH,
          Type.STRING,
          "",
          Importance.LOW,
          CassandraConfigConstants.KEY_STORE_PATH_DOC)

      .define(CassandraConfigConstants.KEY_STORE_PASSWD,
          Type.PASSWORD,
          "",
          Importance.LOW,
          CassandraConfigConstants.KEY_STORE_PASSWD_DOC)
      .define(CassandraConfigConstants.ALLOW_FILTERING,
          Type.BOOLEAN,
          CassandraConfigConstants.ALLOW_FILTERING_DEFAULT,
          Importance.MEDIUM,
          CassandraConfigConstants.ALLOW_FILTERING_DOC)
      .define(CassandraConfigConstants.FETCH_SIZE,
          Type.INT,
          CassandraConfigConstants.FETCH_SIZE_DEFAULT,
          Importance.MEDIUM,
          CassandraConfigConstants.FETCH_SIZE_DOC)
        .define(CassandraConfigConstants.ERROR_POLICY,
          Type.STRING,
          CassandraConfigConstants.ERROR_POLICY_DEFAULT,
          Importance.HIGH,
          CassandraConfigConstants.ERROR_POLICY_DOC
          )
        .define(CassandraConfigConstants.NBR_OF_RETRIES,
          Type.INT,
          CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
          Importance.MEDIUM,
          CassandraConfigConstants.NBR_OF_RETRIES_DOC)
        .define(CassandraConfigConstants.ERROR_RETRY_INTERVAL,
          Type.INT,
          CassandraConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
          Importance.MEDIUM,
          CassandraConfigConstants.ERROR_RETRY_INTERVAL_DOC)

}


/**
  * Holds the extra configurations for the source on top of
  * the base.
  * */
trait CassandraConfigSource extends CassandraConfig {
      val sourceConfig = configDef
            .define(CassandraConfigConstants.IMPORT_MODE,
                    Type.STRING,
                    Importance.HIGH,
                    CassandraConfigConstants.IMPORT_MODE_DOC)
            .define(CassandraConfigConstants.ASSIGNED_TABLES,
                    Type.STRING,
                    Importance.HIGH,
                    CassandraConfigConstants.ASSIGNED_TABLES_DOC)
          .define(CassandraConfigConstants.IMPORT_ROUTE_QUERY,
                    Type.STRING,
                    Importance.HIGH,
                    CassandraConfigConstants.IMPORT_ROUTE_QUERY_DOC)
            .define(CassandraConfigConstants.READER_BUFFER_SIZE,
                    Type.INT,
                    CassandraConfigConstants.READER_BUFFER_SIZE_DEFAULT,
                    Importance.MEDIUM,
                    CassandraConfigConstants.READER_BUFFER_SIZE_DOC)
            .define(CassandraConfigConstants.BATCH_SIZE,
                    Type.INT,
                    CassandraConfigConstants.BATCH_SIZE_DEFAULT,
                    Importance.MEDIUM,
                    CassandraConfigConstants.BATCH_SIZE_DOC)
            .define(CassandraConfigConstants.POLL_INTERVAL,
                    Type.LONG,
                    CassandraConfigConstants.DEFAULT_POLL_INTERVAL,
                    Importance.MEDIUM,
                    CassandraConfigConstants.POLL_INTERVAL_DOC)
        }

/**
  * Holds the extra configurations for the sink on top of
  * the base.
  * */
trait CassandraConfigSink extends CassandraConfig {
  val sinkConfig = configDef
    .define(CassandraConfigConstants.EXPORT_ROUTE_QUERY,
      Type.STRING,
      Importance.HIGH,
      CassandraConfigConstants.EXPORT_ROUTE_QUERY_DOC)

}

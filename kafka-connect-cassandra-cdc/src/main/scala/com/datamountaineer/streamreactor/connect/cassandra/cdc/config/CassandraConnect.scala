/*
 * Copyright 2017 Datamountaineer.
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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Holds the base configuration.
  **/
object CassandraConnect {
  val KCQL = "connect.cassandra.kcql"
  val KCQL_DOC = "Describes via SQL the source Cassandra table and it's target Kafka Topic. For example: INSERT INTO topicOrders SELECT * FROM datamountaineer.orders"
  val KCQL_DISPLAY = "KCQL"

  val CONTACT_POINTS = s"connect.cassandra.contact.points"
  val CONTACT_POINT_DOC = "Initial contact point host for Cassandra including port."
  val CONTACT_POINT_DEFAULT = "127.0.0.1"

  val PORT = s"connect.cassandra.port"
  val PORT_DEFAULT = "9042"
  val PORT_DOC = "Cassandra native port."

  val USERNAME = s"connect.cassandra.username"
  val USERNAME_DOC = "Username to connect to Cassandra with."
  val USERNAME_DEFAULT = "cassandra.cassandra"

  val PASSWD = s"connect.cassandra.password"
  val PASSWD_DOC = "Password for the username to connect to Cassandra with."
  val PASSWD_DEFAULT = "cassandra"

  val SSL_ENABLED = s"connect.cassandra.ssl.enabled"
  val SSL_ENABLED_DOC = "Secure Cassandra driver connection via SSL."
  val SSL_ENABLED_DEFAULT = "false"

  val TRUST_STORE_PATH = s"connect.cassandra.trust.store.path"
  val TRUST_STORE_PATH_DOC = "Path to the client Trust Store."
  val TRUST_STORE_PASSWD = s"connect.cassandra.trust.store.password"
  val TRUST_STORE_PASSWD_DOC = "Password for the client Trust Store."
  val TRUST_STORE_TYPE = s"connect.cassandra.trust.store.type"
  val TRUST_STORE_TYPE_DOC = "Type of the Trust Store, defaults to JKS"
  val TRUST_STORE_TYPE_DEFAULT = "JKS"

  val USE_CLIENT_AUTH = s"connect.cassandra.ssl.client.cert.auth"
  val USE_CLIENT_AUTH_DEFAULT = "false"
  val USE_CLIENT_AUTH_DOC = "Enable client certification authentication by Cassandra. Requires KeyStore options to be set."

  val KEY_STORE_PATH = s"connect.cassandra.key.store.path"
  val KEY_STORE_PATH_DOC = "Path to the client Key Store."
  val KEY_STORE_PASSWD = s"connect.cassandra.key.store.password"
  val KEY_STORE_PASSWD_DOC = "Password for the client Key Store"
  val KEY_STORE_TYPE = s"connect.cassandra.key.store.type"
  val KEY_STORE_TYPE_DOC = "Type of the Key Store, defauts to JKS"
  val KEY_STORE_TYPE_DEFAULT = "JKS"

  val FILE_WATCH_INTERVAL = "connect.cassandra.cdc.file.watch.interval"
  val FILE_WATCH_INTERVAL_DOC = "The delay time in milliseconds before the connector checks for Cassandra CDC files."
  val FILE_WATCH_INTERVAL_DEFAULT = 2000L
  val FILE_WATCH_INTERVAL_DISPLAY = "File watch interval"


  val MUTATION_BUFFER_SIZE = "connect.cassandra.cdc.mutation.queue.size"
  val MUTATION_BUFFER_SIZE_DOC =
    """
      |The maximum number of Cassandra mutation to buffer.
      |As it reads from the Cassandra CDC files the mutations are buffered before they
      |are handed over to Kafka Connect when the framework calls for new records.
    """.stripMargin
  val MUTATION_BUFFER_SIZE_DEFAULT = 1000000
  val MUTATION_BUFFER_SIZE_DISPLAY = "Mutations buffer size"

  val YAML_PATH = "connect.cassandra.yaml.path.url"
  val YAML_PATH_DOC = "The location of the Cassandra Yaml file in URL format:file://{THE_PATH_TO_THE_YAML_FILE}"
  val YAML_PATH_DISPLAY = "Yaml path"


  val ENABLE_FILE_DELETE_WHILE_READING = "connect.cassandra.cdc.enable.delete.while.read"
  val ENABLE_FILE_DELETE_WHILE_READING_DOC =
    """
      |The worker CDC thread will read a CDC file and then check if any of the processed files are
      |ready to be deleted (that means the records have been sent to Kafka). Rather than waiting for a read to complete
      |we can delete the files while reading a CDC file.
      |Default value is true. You can disable it for faster reads by setting the value to false
    """.stripMargin
  val ENABLE_FILE_DELETE_WHILE_READING_DEFAULT = true
  val ENABLE_FILE_DELETE_WHILE_READING_DISPLAY = "Delete files while reading"


  val SINGLE_INSTANCE_PORT = "connect.cassandra.cdc.single.instance.port"
  val SINGLE_INSTANCE_PORT_DOC =
    """
      |Kafka Connect framework doesn't allow yet configuration where you are saying runnning only one task
      |per worker. If you allocate more tasks than workers then some workers will spin up more tasks.
      |With Cassandra nodes we want one worker and one task - not more. To ensure this we allow the first task
      |to grab a port - subsequent calls to open the port will fail thus not allowing multiple instance running at once
    """.stripMargin
  val SINGLE_INSTANCE_PORT_DEFAULT = 64101
  val SINGLE_INSTANCE_PORT_DISPLAY = "Single instance port"

  val DECIMAL_SCALE = "connect.cassandra.cdc.decimal.scale"
  val DECIMAL_SCALE_DOC =
    """
      |When reading the column family metadata we don't have details about the decimal scale.
      |We default to 18.""".stripMargin
  val DECIMAL_SCALE_DEFAULT = 18
  val DECIMAL_SCALE_DISPLAY = "Decimal Scale"

  val configDef: ConfigDef = new ConfigDef()
    .define(CONTACT_POINTS,
      Type.STRING,
      CONTACT_POINT_DEFAULT,
      Importance.HIGH,
      CONTACT_POINT_DOC,
      "Connection",
      1,
      ConfigDef.Width.LONG,
      CONTACT_POINTS
    )

    .define(PORT,
      Type.INT,
      PORT_DEFAULT,
      Importance.HIGH,
      PORT_DOC,
      "Connection",
      2,
      ConfigDef.Width.LONG,
      PORT)

    .define(USERNAME,
      Type.STRING,
      null,
      Importance.HIGH,
      USERNAME_DOC,
      "Connection",
      4,
      ConfigDef.Width.LONG,
      USERNAME)

    .define(PASSWD,
      Type.PASSWORD,
      null,
      Importance.LOW,
      PASSWD_DOC,
      "Connection",
      5,
      ConfigDef.Width.LONG,
      PASSWD
    )

    .define(SSL_ENABLED,
      Type.BOOLEAN,
      SSL_ENABLED_DEFAULT,
      Importance.LOW,
      SSL_ENABLED_DOC,
      "Connection",
      6,
      ConfigDef.Width.LONG,
      SSL_ENABLED)

    .define(TRUST_STORE_PATH,
      Type.STRING,
      "",
      Importance.LOW,
      TRUST_STORE_PATH_DOC,
      "Connection",
      7,
      ConfigDef.Width.LONG,
      TRUST_STORE_PATH)

    .define(TRUST_STORE_PASSWD,
      Type.PASSWORD,
      "",
      Importance.LOW,
      TRUST_STORE_PASSWD_DOC,
      "Connection",
      8,
      ConfigDef.Width.LONG,
      TRUST_STORE_PASSWD)

    .define(TRUST_STORE_TYPE,
      Type.STRING,
      TRUST_STORE_TYPE_DEFAULT,
      Importance.MEDIUM,
      TRUST_STORE_TYPE_DOC,
      "Connection",
      9,
      ConfigDef.Width.MEDIUM,
      TRUST_STORE_TYPE)

    .define(KEY_STORE_TYPE,
      Type.STRING,
      KEY_STORE_TYPE_DEFAULT,
      Importance.MEDIUM,
      KEY_STORE_TYPE_DOC,
      "Connection",
      10,
      ConfigDef.Width.MEDIUM,
      KEY_STORE_TYPE)

    .define(USE_CLIENT_AUTH,
      Type.BOOLEAN,
      USE_CLIENT_AUTH_DEFAULT,
      Importance.LOW,
      USE_CLIENT_AUTH_DOC,
      "Connection",
      11,
      ConfigDef.Width.LONG,
      USE_CLIENT_AUTH)

    .define(KEY_STORE_PATH,
      Type.STRING,
      "",
      Importance.LOW,
      KEY_STORE_PATH_DOC,
      "Connection",
      12,
      ConfigDef.Width.LONG,
      KEY_STORE_PATH)

    .define(KEY_STORE_PASSWD,
      Type.PASSWORD,
      "",
      Importance.LOW,
      KEY_STORE_PASSWD_DOC,
      "Connection",
      13,
      ConfigDef.Width.LONG,
      KEY_STORE_PASSWD)


    .define(FILE_WATCH_INTERVAL,
      Type.LONG,
      FILE_WATCH_INTERVAL_DEFAULT,
      Importance.LOW,
      FILE_WATCH_INTERVAL_DOC,
      "CDC",
      1,
      ConfigDef.Width.MEDIUM,
      FILE_WATCH_INTERVAL_DISPLAY)


    .define(MUTATION_BUFFER_SIZE,
      Type.INT,
      MUTATION_BUFFER_SIZE_DEFAULT,
      Importance.LOW,
      MUTATION_BUFFER_SIZE_DOC,
      "CDC",
      2,
      ConfigDef.Width.MEDIUM,
      MUTATION_BUFFER_SIZE_DISPLAY)


    .define(YAML_PATH,
      Type.STRING,
      Importance.LOW,
      YAML_PATH_DOC,
      "CDC",
      3,
      ConfigDef.Width.MEDIUM,
      YAML_PATH_DISPLAY)


    .define(ENABLE_FILE_DELETE_WHILE_READING,
      Type.BOOLEAN,
      ENABLE_FILE_DELETE_WHILE_READING_DEFAULT,
      Importance.LOW,
      ENABLE_FILE_DELETE_WHILE_READING_DOC,
      "CDC",
      4,
      ConfigDef.Width.MEDIUM,
      ENABLE_FILE_DELETE_WHILE_READING_DISPLAY)

    .define(SINGLE_INSTANCE_PORT,
      Type.INT,
      SINGLE_INSTANCE_PORT_DEFAULT,
      Importance.LOW,
      SINGLE_INSTANCE_PORT_DOC,
      "CDC",
      5,
      ConfigDef.Width.MEDIUM,
      SINGLE_INSTANCE_PORT_DISPLAY)

    .define(DECIMAL_SCALE,
      Type.INT,
      DECIMAL_SCALE_DEFAULT,
      Importance.LOW,
      DECIMAL_SCALE_DOC,
      "CDC",
      6,
      ConfigDef.Width.MEDIUM,
      DECIMAL_SCALE_DISPLAY)

    .define(KCQL,
      Type.STRING,
      Importance.LOW,
      KCQL_DOC,
      "KCQL",
      1,
      ConfigDef.Width.LONG,
      KCQL_DISPLAY)
}


case class CassandraConnect(props: util.Map[String, String]) extends AbstractConfig(CassandraConnect.configDef, props)

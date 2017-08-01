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

/**
  * Holds the Cassandra connection details
  *
  * @param contactPoints - The cassandra connection contact points
  * @param port          - The cassandra connection port
  * @param yamlPath      - The path for the cassandra yaml config file
  * @param user          - Optional cassandra connection user
  * @param password      - Optional cassandra connection password
  * @param ssl           - Optional ssl settings for a secure connection
  */
case class CassandraConfig(contactPoints: String,
                           port: Int,
                           yamlPath: String,
                           user: Option[String],
                           password: Option[String],
                           ssl: Option[SSLConfig])


object CassandraConfig {
  def apply(connect: CassandraConnect): CassandraConfig = {
    val contactPoints = connect.getString(CassandraConnect.CONTACT_POINTS)
    val port = connect.getInt(CassandraConnect.PORT)
    val yamlPath = connect.getString(CassandraConnect.YAML_PATH)

    val user = Option(connect.getString(CassandraConnect.USERNAME))
    val password = Option(connect.getString(CassandraConnect.PASSWD))

    new CassandraConfig(
      contactPoints,
      port,
      yamlPath,
      user,
      password,
      SSLConfig(connect)
    )
  }
}
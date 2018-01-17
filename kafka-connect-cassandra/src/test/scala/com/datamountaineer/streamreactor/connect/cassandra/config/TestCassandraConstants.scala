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

package com.datamountaineer.streamreactor.connect.cassandra.config

import org.scalatest.{Matchers, WordSpec}

/**
  * The point of this test is to check that constants keys are not changed after the refactor of the code.
  */
class TestCassandraConstants extends WordSpec with Matchers {

  // Constants
  val POLL_INTERVAL = "connect.cassandra.import.poll.interval"
  val KEY_SPACE = "connect.cassandra.key.space"
  val CONTACT_POINTS = "connect.cassandra.contact.points"
  val PORT = "connect.cassandra.port"
  val USERNAME = "connect.cassandra.username"
  val PASSWD = "connect.cassandra.password"
  val SSL_ENABLED = "connect.cassandra.ssl.enabled"
  val TRUST_STORE_PATH = "connect.cassandra.trust.store.path"
  val TRUST_STORE_PASSWD = "connect.cassandra.trust.store.password"
  val TRUST_STORE_TYPE = "connect.cassandra.trust.store.type"
  val USE_CLIENT_AUTH = "connect.cassandra.ssl.client.cert.auth"
  val KEY_STORE_PATH = "connect.cassandra.key.store.path"
  val KEY_STORE_PASSWD = "connect.cassandra.key.store.password"
  val KEY_STORE_TYPE = "connect.cassandra.key.store.type"
  val BATCH_SIZE = "connect.cassandra.batch.size"
  val READER_BUFFER_SIZE = "connect.cassandra.task.buffer.size"
  val ALLOW_FILTERING = "connect.cassandra.import.allow.filtering"
  val ASSIGNED_TABLES = "connect.cassandra.assigned.tables"
  val ERROR_POLICY = "connect.cassandra.error.policy"
  val ERROR_RETRY_INTERVAL = "connect.cassandra.retry.interval"
  val NBR_OF_RETRIES = "connect.cassandra.max.retries"
  val ROUTE_QUERY = "connect.cassandra.kcql"
  val THREAD_POOL_CONFIG = "connect.cassandra.threadpool.size"
  val CONSISTENCY_LEVEL_CONFIG = "connect.cassandra.consistency.level"
  val MAPPING_COLLECTION_TO_JSON = "connect.cassandra.mapping.collection.to.json"

  "POLL_INTERVAL should have the same key in CassandraConfigConstants" in {
    assert(POLL_INTERVAL.equals(CassandraConfigConstants.POLL_INTERVAL))
  }
  "KEY_SPACE should have the same key in CassandraConfigConstants" in {
    assert(KEY_SPACE.equals(CassandraConfigConstants.KEY_SPACE))
  }
  "CONTACT_POINTS should have the same key in CassandraConfigConstants" in {
    assert(CONTACT_POINTS.equals(CassandraConfigConstants.CONTACT_POINTS))
  }
  "PORT should have the same key in CassandraConfigConstants" in {
    assert(PORT.equals(CassandraConfigConstants.PORT))
  }
  "USERNAME should have the same key in CassandraConfigConstants" in {
    assert(USERNAME.equals(CassandraConfigConstants.USERNAME))
  }
  "PASSWD should have the same key in CassandraConfigConstants" in {
    assert(PASSWD.equals(CassandraConfigConstants.PASSWD))
  }
  "SSL_ENABLED should have the same key in CassandraConfigConstants" in {
    assert(SSL_ENABLED.equals(CassandraConfigConstants.SSL_ENABLED))
  }
  "TRUST_STORE_PATH should have the same key in CassandraConfigConstants" in {
    assert(TRUST_STORE_PATH.equals(CassandraConfigConstants.TRUST_STORE_PATH))
  }
  "TRUST_STORE_PASSWD should have the same key in CassandraConfigConstants" in {
    assert(TRUST_STORE_PASSWD.equals(CassandraConfigConstants.TRUST_STORE_PASSWD))
  }
  "TRUST_STORE_TYPE should have the same key in CassandraConfigConstants" in {
    assert(TRUST_STORE_TYPE.equals(CassandraConfigConstants.TRUST_STORE_TYPE))
  }
  "USE_CLIENT_AUTH should have the same key in CassandraConfigConstants" in {
    assert(USE_CLIENT_AUTH.equals(CassandraConfigConstants.USE_CLIENT_AUTH))
  }
  "KEY_STORE_PATH should have the same key in CassandraConfigConstants" in {
    assert(KEY_STORE_PATH.equals(CassandraConfigConstants.KEY_STORE_PATH))
  }
  "KEY_STORE_PASSWD should have the same key in CassandraConfigConstants" in {
    assert(KEY_STORE_PASSWD.equals(CassandraConfigConstants.KEY_STORE_PASSWD))
  }
  "KEY_STORE_TYPE should have the same key in CassandraConfigConstants" in {
    assert(KEY_STORE_TYPE.equals(CassandraConfigConstants.KEY_STORE_TYPE))
  }
  "BATCH_SIZE should have the same key in CassandraConfigConstants" in {
    assert(BATCH_SIZE.equals(CassandraConfigConstants.BATCH_SIZE))
  }
  "READER_BUFFER_SIZE should have the same key in CassandraConfigConstants" in {
    assert(READER_BUFFER_SIZE.equals(CassandraConfigConstants.READER_BUFFER_SIZE))
  }
  "ALLOW_FILTERING should have the same key in CassandraConfigConstants" in {
    assert(ALLOW_FILTERING.equals(CassandraConfigConstants.ALLOW_FILTERING))
  }
  "ASSIGNED_TABLES should have the same key in CassandraConfigConstants" in {
    assert(ASSIGNED_TABLES.equals(CassandraConfigConstants.ASSIGNED_TABLES))
  }
  "ERROR_POLICY should have the same key in CassandraConfigConstants" in {
    assert(ERROR_POLICY.equals(CassandraConfigConstants.ERROR_POLICY))
  }
  "ERROR_RETRY_INTERVAL should have the same key in CassandraConfigConstants" in {
    assert(ERROR_RETRY_INTERVAL.equals(CassandraConfigConstants.ERROR_RETRY_INTERVAL))
  }
  "NBR_OF_RETRIES should have the same key in CassandraConfigConstants" in {
    assert(NBR_OF_RETRIES.equals(CassandraConfigConstants.NBR_OF_RETRIES))
  }
  "ROUTE_QUERY should have the same key in CassandraConfigConstants" in {
    assert(ROUTE_QUERY.equals(CassandraConfigConstants.KCQL))
  }
  "THREAD_POOL_CONFIG should have the same key in CassandraConfigConstants" in {
    assert(THREAD_POOL_CONFIG.equals(CassandraConfigConstants.THREAD_POOL_CONFIG))
  }
  "CONSISTENCY_LEVEL_CONFIG should have the same key in CassandraConfigConstants" in {
    assert(CONSISTENCY_LEVEL_CONFIG.equals(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG))
  }
  "MAPPING_COLLECTION_TO_JSON should have the same key in CassandraConfigConstants" in {
    assert(MAPPING_COLLECTION_TO_JSON.equals(CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON))
  }
}

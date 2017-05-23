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

package com.datamountaineer.streamreactor.connect.rethink.config

import com.datamountaineer.streamreactor.connect.rethink.TestBase

/**
  * The point of this test is to check that constants keys are not changed after the refactor of the code.
  */
class TestReThinkSinkConstants extends TestBase {

  // Constants
  val RETHINK_HOST = "connect.rethink.sink.host"
  val RETHINK_DB = "connect.rethink.db"
  val RETHINK_PORT = "connect.rethink.sink.port"
  val EXPORT_ROUTE_QUERY = "connect.rethink.kcql"
  val ERROR_POLICY = "connect.rethink.error.policy"
  val ERROR_RETRY_INTERVAL = "connect.rethink.retry.interval"
  val NBR_OF_RETRIES = "connect.rethink.max.retries"
  val BATCH_SIZE = "connect.rethink.batch.size"

  "RETHINK_HOST should have the same key in ReThinkSinkConfigConstants" in {
    assert(RETHINK_HOST.equals(ReThinkSinkConfigConstants.RETHINK_HOST))
  }

  "RETHINK_DB should have the same key in ReThinkSinkConfigConstants" in {
    assert(RETHINK_DB.equals(ReThinkSinkConfigConstants.RETHINK_DB))
  }

  "RETHINK_PORT should have the same key in ReThinkSinkConfigConstants" in {
    assert(RETHINK_PORT.equals(ReThinkSinkConfigConstants.RETHINK_PORT))
  }

  "EXPORT_ROUTE_QUERY should have the same key in ReThinkSinkConfigConstants" in {
    assert(EXPORT_ROUTE_QUERY.equals(ReThinkSinkConfigConstants.EXPORT_ROUTE_QUERY))
  }

  "ERROR_POLICY should have the same key in ReThinkSinkConfigConstants" in {
    assert(ERROR_POLICY.equals(ReThinkSinkConfigConstants.ERROR_POLICY))
  }

  "ERROR_RETRY_INTERVAL should have the same key in ReThinkSinkConfigConstants" in {
    assert(ERROR_RETRY_INTERVAL.equals(ReThinkSinkConfigConstants.ERROR_RETRY_INTERVAL))
  }

  "NBR_OF_RETRIES should have the same key in ReThinkSinkConfigConstants" in {
    assert(NBR_OF_RETRIES.equals(ReThinkSinkConfigConstants.NBR_OF_RETRIES))
  }

  "BATCH_SIZE should have the same key in ReThinkSinkConfigConstants" in {
    assert(BATCH_SIZE.equals(ReThinkSinkConfigConstants.BATCH_SIZE))
  }
}

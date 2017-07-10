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

package com.datamountaineer.streamreactor.connect.rethink

import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSourceConfig}
import com.rethinkdb.RethinkDB
import org.apache.kafka.connect.errors.ConnectException

import org.scalatest.mock.MockitoSugar

/**
  * Created by andrew@datamountaineer.com on 29/05/2017. 
  * stream-reactor
  */
class TestReThinkConnection extends TestBase with MockitoSugar {


  "should throw exception if cert is set and no auth key" in {
    val r = mock[RethinkDB]
    val props = getPropsConnTestNoAuth
    val config = ReThinkSourceConfig(props)

    intercept[ConnectException] {
      ReThinkConnection(r, config)
    }
  }

  "should throw exception if auth key is set and no cert" in {
    val r = mock[RethinkDB]
    val props = getPropsConnTestNoAuth
    val config = ReThinkSourceConfig(props)

    intercept[ConnectException] {
      ReThinkConnection(r, config)
    }
  }
}

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

import com.datamountaineer.connector.config.WriteModeEnum
import com.datamountaineer.streamreactor.connect.rethink.TestBase
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 21/06/16. 
  * stream-reactor-maven
  */
class TestReThinkSinkSettings extends TestBase with MockitoSugar {
  "should create a RethinkSetting for INSERT with all fields" in {
    val config = ReThinkSinkConfig(getProps)
    val settings = ReThinkSinkSettings(config)
    val routes = settings.routes.head
    routes.getSource shouldBe TOPIC
    routes.getTarget shouldBe TABLE
    routes.getWriteMode shouldBe WriteModeEnum.INSERT
    val conflict = settings.conflictPolicy(TABLE)
    conflict shouldBe ReThinkSinkConfigConstants.CONFLICT_ERROR
    routes.isIncludeAllFields shouldBe true
  }

  "should create a RethinkSetting for UPSERT with fields selection with RETRY" in {
    val config = ReThinkSinkConfig(getPropsUpsertSelectRetry)
    val settings = ReThinkSinkSettings(config)
    val routes = settings.routes.head
    routes.getSource shouldBe TOPIC
    routes.getTarget shouldBe TABLE
    routes.getWriteMode shouldBe WriteModeEnum.UPSERT
    val conflict = settings.conflictPolicy(TABLE)
    conflict shouldBe ReThinkSinkConfigConstants.CONFLICT_REPLACE
    routes.isIncludeAllFields shouldBe false
    val fields = routes.getFieldAlias.asScala.toList
    fields.size shouldBe 2
  }
}

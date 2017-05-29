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

package com.datamountaineer.streamreactor.connect.rethink.source

import com.datamountaineer.streamreactor.connect.rethink.TestBase
import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkSourceConfig
import org.apache.kafka.connect.data.Struct

/**
  * Created by andrew@datamountaineer.com on 23/09/16. 
  * stream-reactor
  */
class TestReThinkSourceReader extends TestBase with MockReThinkSource {
  "reader should read change feeds" in {
    val props = getPropsSource
    val config = ReThinkSourceConfig(props)
    val readers = ReThinkSourceReadersFactory(config, r)
    readers.foreach(_.start())
    val reader = readers.head
    while (reader.queue.size() == 0) {
      Thread.sleep(100)
    }
    val record = reader.queue.take()
    val struct = record.value().asInstanceOf[Struct]
    struct.getString("new_val") shouldBe (newVal)
    struct.getString("old_val") shouldBe oldVal
    struct.getString("type") shouldBe `type`
  }
}

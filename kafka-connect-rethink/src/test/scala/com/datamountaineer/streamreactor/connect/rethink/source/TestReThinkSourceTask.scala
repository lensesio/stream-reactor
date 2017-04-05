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


/**
  * Created by andrew@datamountaineer.com on 23/09/16. 
  * stream-reactor
  */
class TestReThinkSourceTask extends TestBase with MockReThinkSource {
  "should start a task and readers" in {
    val props = getPropsSource
    val config = ReThinkSourceConfig(props)
    val task = new ReThinkSourceTask
    task.startReaders(config, r)
    Thread.sleep(3000)
    (task.poll().size() > 0) shouldBe true
    task.stop()
  }
}

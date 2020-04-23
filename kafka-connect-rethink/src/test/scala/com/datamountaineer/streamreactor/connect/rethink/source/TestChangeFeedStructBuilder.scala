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
/**
  * Created by andrew@datamountaineer.com on 23/09/16. 
  * stream-reactor
  */
class TestChangeFeedStructBuilder extends TestBase {

  "should convert a initial changefeed to a struct" in {
    val newVal = "{\"name\",\"datamountaineer\"}"
    val hs = Map("new_val"->newVal, "type"->"initial")

    val out = ChangeFeedStructBuilder(hs)
    out.getString("new_val") shouldBe newVal
    out.getString("type") shouldBe "initial"
  }

  "should convert a ready state changefeed to a struct" in {
    val hs = Map("state"->"ready", "type"->"state")

    val out = ChangeFeedStructBuilder(hs)
    out.getString("state") shouldBe "ready"
    out.getString("type") shouldBe "state"
  }

  "should convert a new record changefeed to a struct" in {
    val newVal = "{\"name\",\"datamountaineer\"}"
    val oldVal : java.util.HashMap[String, Object] = null
    val hs : Map[String, Object] = Map("new_val"->newVal, "type"->"add", "old_val"->oldVal)

    val out = ChangeFeedStructBuilder(hs)
    out.getString("new_val") shouldBe newVal
    out.getString("type") shouldBe "add"
    out.get("old_val") shouldBe null
  }
}

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

import akka.actor.ActorSystem
import com.datamountaineer.streamreactor.connect.rethink.TestBase
import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkSourceConfig
import com.datamountaineer.streamreactor.connect.rethink.source.ReThinkSourceReader.StartChangeFeed
import org.apache.kafka.connect.data.Struct
import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 23/09/16. 
  * stream-reactor
  */
class TestReThinkSourceReader extends TestBase with MockReThinkSource {
  "reader should read changefeeds" in {
    implicit val system = ActorSystem()

    val props = getPropsSource
    val config = ReThinkSourceConfig(props)
    val actorProps = ReThinkSourceReader(config, r)
    val actors = actorProps.map({ case (source, prop) => system.actorOf(prop, source) }).toSet
    actors.foreach(_ ! StartChangeFeed)
    Thread.sleep(1000)
    val records = actors.flatMap(ActorHelper.askForRecords)
    val struct = records.head.value().asInstanceOf[Struct]
    struct.getString("new_val") shouldBe (newVal)
    struct.getString("old_val") shouldBe oldVal
    struct.getString("type") shouldBe `type`
  }
}

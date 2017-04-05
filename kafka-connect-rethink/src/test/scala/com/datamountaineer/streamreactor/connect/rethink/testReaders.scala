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

///*
// * *
// *   * Copyright 2016 Datamountaineer.
// *   *
// *   * Licensed under the Apache License, Version 2.0 (the "License");
// *   * you may not use this file except in compliance with the License.
// *   * You may obtain a copy of the License at
// *   *
// *   * http://www.apache.org/licenses/LICENSE-2.0
// *   *
// *   * Unless required by applicable law or agreed to in writing, software
// *   * distributed under the License is distributed on an "AS IS" BASIS,
// *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *   * See the License for the specific language governing permissions and
// *   * limitations under the License.
// *   *
// */
//
//package com.datamountaineer.streamreactor.connect.rethink
//
//import java.util.{Timer, TimerTask}
//import java.util.concurrent.LinkedBlockingQueue
//
//import com.datamountaineer.connector.config.Config
//import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
//import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSourceConfig, ReThinkSourceSettings}
//import com.datamountaineer.streamreactor.connect.rethink.source.ReThinkSourceReader
//import com.rethinkdb.RethinkDB
//import com.rethinkdb.net.Connection
//import org.apache.kafka.connect.source.SourceRecord
//import scala.collection.JavaConversions._
//
///**
//  * Created by andrew@datamountaineer.com on 23/09/16.
//  * stream-reactor
//  */
//object testReaders extends TestBase {
//  private var queues: Seq[LinkedBlockingQueue[SourceRecord]] = _
//
//  class LoggerTask extends TimerTask {
//    override def run(): Unit = {
//      val records = queues.map(q => QueueHelpers.drainQueue(q, q.size())).flatten
//      records.foreach(println)
//    }
//  }
//
//
//  def main(args: Array[String]): Unit = {
//    val timer = new Timer()
//    val props = getPropsSource
//    val config = ReThinkSourceConfig(props)
//    val rethinkHost = config.getString(ReThinkSourceConfig.RETHINK_HOST)
//    val port = config.getInt(ReThinkSourceConfig.RETHINK_PORT)
//
//    //set up the connection to the host
//    val settings = ReThinkSourceSettings(config)
//    lazy val r = RethinkDB.r
//    val conn = Some(r.connection().hostname(rethinkHost).port(port).connect())
//
//    queues = settings.routes.map(routes => new LinkedBlockingQueue[SourceRecord](routes.getBatchSize)).toSeq
//    val routes: Set[(Config, LinkedBlockingQueue[SourceRecord])] = settings.routes zip queues
//    val readers = routes.map({ case (route, queue) => ReaderBuilder(r, conn.get, route, queue)}).toSeq
//    readers.foreach(_.start())
//
//  }
//
//  def ReaderBuilder(rethinkDB: RethinkDB, connection: Connection, route: Config, queue: LinkedBlockingQueue[SourceRecord]) = {
//    val reader = new ReThinkSourceReader(rethinkDB, connection, route, queue)
//    new Thread(reader)
//  }
//}

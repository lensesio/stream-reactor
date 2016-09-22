/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.rethink.source

import java.util

import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSourceConfig, ReThinkSourceSettings}
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
class ReThinkSourceTask extends SourceTask with StrictLogging {
  private var readers : Seq[ReThinkSourceReader] = _
  private var conn: Option[Connection] = None

  override def start(props: util.Map[String, String]) = {
    logger.info(
      """
        |    ____        __        __  ___                  __        _
        |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
        |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
        | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
        |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
        |         ____     ________    _       __   ____  ____ _____
        |        / __ \___/_  __/ /_  (_)___  / /__/ __ \/ __ ) ___/____  __  _______________
        |       / /_/ / _ \/ / / __ \/ / __ \/ //_/ / / / __  \__ \/ __ \/ / / / ___/ ___/ _ \
        |      / _, _/  __/ / / / / / / / / / ,< / /_/ / /_/ /__/ / /_/ / /_/ / /  / /__/  __/
        |     /_/ |_|\___/_/ /_/ /_/_/_/ /_/_/|_/_____/_____/____/\____/\__,_/_/   \___/\___/
        |
        |     By Andrew Stevenson
      """.stripMargin)

    val config = ReThinkSourceConfig(props)
    val rethinkHost = config.getString(ReThinkSourceConfig.RETHINK_HOST)
    val port = config.getInt(ReThinkSourceConfig.RETHINK_PORT)

    //set up the connection to the host
    val settings = ReThinkSourceSettings(config)
    lazy val r = RethinkDB.r
    conn = Some(r.connection().hostname(rethinkHost).port(port).connect())

    readers  = settings.routes.map(route => new ReThinkSourceReader(rethink = r, conn = conn.get, route = route)).toSeq
    readers.foreach(_.run())
  }

  /**
    * Read from readers queue
    * */
  override def poll(): util.List[SourceRecord] =  readers.map(_.get()).flatten.asJava

  /**
    * Shutdown connections
    * */
  override def stop(): Unit = {
    logger.info("Stopping ReThink source and closing connections.")
    conn.foreach(_.close())
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}

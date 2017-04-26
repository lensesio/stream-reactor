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

import java.util

import com.datamountaineer.streamreactor.connect.rethink.TestBase
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSourceConfig, ReThinkSourceConfigConstants}
import com.rethinkdb.RethinkDB
import com.rethinkdb.gen.ast.{Changes, Db, Table}
import com.rethinkdb.net.{Connection, Cursor}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

/**
  * Created by andrew@datamountaineer.com on 29/09/16. 
  * stream-reactor
  */
trait MockReThinkSource extends TestBase with MockitoSugar {
  val r = mock[RethinkDB]
  val conn = mock[Connection]
  val connBuilder = mock[Connection.Builder]
  val db = mock[Db]
  val changes = mock[Changes]
  val table = mock[Table]
  val cursor = mock[Cursor[util.HashMap[String, String]]]
  val hs = new java.util.HashMap[String, String]
  val newVal = "{\"name\",\"datamountaineer\"}"
  val oldVal = "{\"name\",\"foo\"}"
  val `type` = "add"
  hs.put("new_val",newVal)
  hs.put("type",`type`)
  hs.put("old_val", oldVal)

  when(connBuilder.hostname(ReThinkSourceConfigConstants.RETHINK_HOST_DEFAULT)).thenReturn(connBuilder)
  when(connBuilder.port(ReThinkSourceConfigConstants.RETHINK_PORT_DEFAULT.toInt)).thenReturn(connBuilder)
  when(connBuilder.connect()).thenReturn(conn)
  when(r.connection()).thenReturn(connBuilder)
  when(changes.optArg("include_states", true)).thenReturn(changes)
  when(changes.optArg("include_initial", true)).thenReturn(changes)
  when(changes.optArg("include_types", true)).thenReturn(changes)
  when(r.db(DB)).thenReturn(db)
  when(db.table(TABLE)).thenReturn(table)
  when(table.changes()).thenReturn(changes)
  when(changes.run(conn)).thenReturn(cursor)
  when(cursor.hasNext).thenReturn(true)
  when(cursor.next()).thenReturn(hs)
}

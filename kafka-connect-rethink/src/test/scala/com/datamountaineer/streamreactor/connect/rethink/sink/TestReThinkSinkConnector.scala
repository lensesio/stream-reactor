package com.datamountaineer.streamreactor.connect.rethink.sink

import com.datamountaineer.streamreactor.connect.rethink.TestBase
import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkSinkConfig
import com.rethinkdb.RethinkDB
import com.rethinkdb.gen.ast.{Db, TableCreate, TableList}
import com.rethinkdb.net.Connection

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/06/16. 
  * stream-reactor-maven
  */
class TestReThinkSinkConnector extends TestBase with MockitoSugar{
  "Should start a ReThink Connector" in {

    val r = mock[RethinkDB]
    val conn = mock[Connection]
    val connBuilder = mock[Connection.Builder]
    val db =  mock[Db]
    val tableList = mock[TableList]
    val tableCreate = mock[TableCreate]

    when(connBuilder.hostname(ReThinkSinkConfig.RETHINK_HOST_DEFAULT)).thenReturn(connBuilder)
    when(connBuilder.port(ReThinkSinkConfig.RETHINK_PORT_DEFAULT.toInt)).thenReturn(connBuilder)
    when(connBuilder.connect()).thenReturn(conn)
    when(r.connection()).thenReturn(connBuilder)
    when(r.db(DB)).thenReturn(db)
    when(db.tableList()).thenReturn(tableList)
    when(r.db(DB).tableList().run(conn)).thenReturn(List(TABLE).asJava)
    when(r.db(DB).tableCreate(TABLE)).thenReturn(tableCreate)
    when(tableCreate.optArg(any[String], any[String])).thenReturn(tableCreate)

    val config = getProps
    val connector = new ReThinkSinkConnector()
    connector.initializeTables(r, config)
    connector.taskClass() shouldBe classOf[ReThinkSinkTask]
    connector.stop()
  }
}

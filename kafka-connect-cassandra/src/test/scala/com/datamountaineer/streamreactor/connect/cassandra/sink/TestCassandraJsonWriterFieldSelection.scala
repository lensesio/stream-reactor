package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigSink
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 04/05/16. 
  * stream-reactor
  */
class TestCassandraJsonWriterFieldSelection extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter with TestConfig
  with CassandraConfigSink {
  before {
    startEmbeddedCassandra()
  }

  "Cassandra JsonWriter should write records to Cassandra" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val testRecords = getTestRecords(TABLE1)
    //get config
    val props  = getCassandraConfigSinkPropsFieldSelection
    val taskConfig = new AbstractConfig(sinkConfig, props)

    val writer = CassandraWriter(taskConfig, context)
    writer.write(testRecords)
    Thread.sleep(2000)
    //check we can get back what we wrote
    val res = session.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TABLE1")
    val rs = res.all().asScala


    //check we the columns we wanted
    rs.foreach({
      r=>{
        r.getString("id")
        r.getInt("int_field")
        r.getLong("long_field")
        intercept[IllegalArgumentException] { r.getString("float_field") }
      }
    })

    rs.size shouldBe testRecords.size
  }
}

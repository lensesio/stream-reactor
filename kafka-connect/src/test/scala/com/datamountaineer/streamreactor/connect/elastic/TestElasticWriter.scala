package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.cassandra.{CassandraWriter, CassandraSinkConfig}
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class TestElasticWriter extends TestElasticBase  with MockitoSugar  {
  test("A ElasticWriter should insert into Elastic Search a number of records") {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config  = new ElasticSinkConfig(getElasticSinkConfigProps)
    //get writer
    val writer = ElasticWriter(config = config, context = context)
    //write records to cassandra
    writer.write(testRecords)
    //close writer
    writer.close()
//    //check we can get back what we wrote
//    val res = session.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TOPIC")
//    res.all().size() shouldBe testRecords.size
  }

}
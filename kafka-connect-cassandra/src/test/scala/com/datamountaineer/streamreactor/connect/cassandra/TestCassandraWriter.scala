//package com.datamountaineer.streamreactor.connect.cassandra
//
//import org.apache.kafka.connect.sink.SinkTaskContext
//import org.mockito.Mockito._
//import org.scalatest.mock.MockitoSugar
//
//class TestCassandraWriter extends TestCassandraBase  with MockitoSugar  {
//
//  test("A CassandraWriter should insert into Cassandra a number of records") {
//    //mock the context to return our assignment when called
//    val context = mock[SinkTaskContext]
//    when(context.assignment()).thenReturn(getAssignment)
//    //get test records
//    val testRecords = getTestRecords
//    //get config
//    val config  = new CassandraSinkConfig(getCassandraSinkConfigProps)
//    //get writer
//    val writer = CassandraWriter(connectorConfig = config, context = context)
//    //write records to cassandra
//    writer.write(testRecords)
//    //close writer
//    writer.close()
//    //check we can get back what we wrote
//    val res = SESSION.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TOPIC")
//    res.all().size() shouldBe testRecords.size
//  }
//}

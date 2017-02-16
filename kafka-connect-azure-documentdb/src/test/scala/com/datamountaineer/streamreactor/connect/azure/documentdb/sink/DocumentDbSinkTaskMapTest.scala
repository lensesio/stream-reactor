package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import com.datamountaineer.streamreactor.connect.azure.documentdb.config.DocumentDbConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.azure.documentdb._
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.{eq => mockEq, _}
import org.mockito.Mockito.{verify, _}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class DocumentDbSinkTaskMapTest extends WordSpec with Matchers with MockitoSugar {
  private val connection = "https://accountName.documents.azure.com:443/"

  implicit val formats = org.json4s.DefaultFormats
  private val mapper = new ObjectMapper()
  private val `type` = mapper.getTypeFactory.constructMapType(classOf[java.util.HashMap[String, Any]], classOf[String], classOf[Any])
  
  "DocumentDbSinkTask" should {
    "handle util.Map INSERTS with default consistency level" in {
      val map = Map(
        DocumentDbConfig.DATABASE_CONFIG -> "database1",
        DocumentDbConfig.CONNECTION_CONFIG -> connection,
        DocumentDbConfig.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfig.KCQL_CONFIG -> "INSERT INTO coll1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topic2"
      )

      val documentClient = mock[DocumentClient]
      val dbResource: ResourceResponse[Database] = mock[ResourceResponse[Database]]
      when(dbResource.getResource).thenReturn(mock[Database])

      Seq("coll1", "coll2").foreach { c =>
        val resource = mock[ResourceResponse[DocumentCollection]]
        when(resource.getResource).thenReturn(mock[DocumentCollection])

        when(documentClient.readCollection(mockEq(c), any(classOf[RequestOptions])))
          .thenReturn(resource)

        when(documentClient.readCollection(mockEq(c), mockEq(null)))
          .thenReturn(resource)
      }

      when(documentClient.readDatabase(mockEq("database1"), any(classOf[RequestOptions])))
        .thenReturn(dbResource)

      val task = new DocumentDbSinkTask(s => documentClient)
      task.start(map)

      val json1 = scala.io.Source.fromFile(getClass.getResource(s"/transaction1.json").toURI.getPath).mkString
      val map1: java.util.Map[String, Any] = mapper.readValue(json1, `type`)

      val json2 = scala.io.Source.fromFile(getClass.getResource(s"/transaction2.json").toURI.getPath).mkString
      val map2: java.util.Map[String, Any] = mapper.readValue(json2, `type`)

      val sinkRecord1 = new SinkRecord("topic1", 0, null, null, null, map1, 1000)
      val sinkRecord2 = new SinkRecord("topic2", 0, null, null, null, map2, 1000)

      val doc1 = new Document(json1)
      val r1 = mock[ResourceResponse[Document]]
      when(r1.getResource).thenReturn(doc1)

      when(documentClient.createDocument(mockEq("coll1"), argThat {
        new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument != null && argument.toString == doc1.toString
          }
        }
      }, argThat {
        new ArgumentMatcher[RequestOptions] {
          override def matches(argument: RequestOptions): Boolean = argument.getConsistencyLevel == ConsistencyLevel.Session
        }
      }, mockEq(false)))
        .thenReturn(r1)

      val doc2 = new Document(json2)
      val r2 = mock[ResourceResponse[Document]]
      when(r2.getResource).thenReturn(doc2)

      when(documentClient.createDocument(mockEq("coll2"), argThat {
        new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument != null && argument.toString == doc2.toString
          }
        }
      }, argThat {
        new ArgumentMatcher[RequestOptions] {
          override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Session
        }
      }, mockEq(false)))
        .thenReturn(r2)

      task.put(Seq(sinkRecord1, sinkRecord2))

      verify(documentClient).createDocument(mockEq("coll1"),
        argThat(new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument.toString == doc1.toString
          }
        }), argThat {
          new ArgumentMatcher[RequestOptions] {
            override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Session
          }
        }, mockEq(false))

      verify(documentClient).createDocument(mockEq("coll2"),
        argThat(new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            doc2.toString == argument.toString
          }
        }), argThat {
          new ArgumentMatcher[RequestOptions] {
            override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Session
          }
        }
        , mockEq(false))
    }

    "handle util.Map INSERTS with Eventual consistency level" in {
      val map = Map(
        DocumentDbConfig.DATABASE_CONFIG -> "database1",
        DocumentDbConfig.CONNECTION_CONFIG -> connection,
        DocumentDbConfig.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfig.CONSISTENCY_CONFIG -> ConsistencyLevel.Eventual.toString,
        DocumentDbConfig.KCQL_CONFIG -> "INSERT INTO coll1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topic2"
      )

      val documentClient = mock[DocumentClient]
      val dbResource: ResourceResponse[Database] = mock[ResourceResponse[Database]]
      when(dbResource.getResource).thenReturn(mock[Database])

      Seq("coll1", "coll2").foreach { c =>
        val resource = mock[ResourceResponse[DocumentCollection]]
        when(resource.getResource).thenReturn(mock[DocumentCollection])

        when(documentClient.readCollection(mockEq(c), any(classOf[RequestOptions])))
          .thenReturn(resource)

        when(documentClient.readCollection(mockEq(c), mockEq(null)))
          .thenReturn(resource)
      }

      when(documentClient.readDatabase(mockEq("database1"), any(classOf[RequestOptions])))
        .thenReturn(dbResource)

      val task = new DocumentDbSinkTask(s => documentClient)
      task.start(map)

      val json1 = scala.io.Source.fromFile(getClass.getResource(s"/transaction1.json").toURI.getPath).mkString
      val map1: java.util.Map[String, Any] = mapper.readValue(json1, `type`)

      val json2 = scala.io.Source.fromFile(getClass.getResource(s"/transaction2.json").toURI.getPath).mkString
      val map2: java.util.Map[String, Any] = mapper.readValue(json2, `type`)

      val sinkRecord1 = new SinkRecord("topic1", 0, null, null, null, map1, 1000)
      val sinkRecord2 = new SinkRecord("topic2", 0, null, null, null, map2, 1000)

      val doc1 = new Document(json1)
      val r1 = mock[ResourceResponse[Document]]
      when(r1.getResource).thenReturn(doc1)

      when(documentClient.createDocument(mockEq("coll1"), argThat {
        new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument != null && argument.toString == doc1.toString
          }
        }
      }, argThat {
        new ArgumentMatcher[RequestOptions] {
          override def matches(argument: RequestOptions): Boolean = argument.getConsistencyLevel == ConsistencyLevel.Eventual
        }
      }, mockEq(false)))
        .thenReturn(r1)

      val doc2 = new Document(json2)
      val r2 = mock[ResourceResponse[Document]]
      when(r2.getResource).thenReturn(doc2)

      when(documentClient.createDocument(mockEq("coll2"), argThat {
        new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument != null && argument.toString == doc2.toString
          }
        }
      }, argThat {
        new ArgumentMatcher[RequestOptions] {
          override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Eventual
        }
      }, mockEq(false)))
        .thenReturn(r2)

      task.put(Seq(sinkRecord1, sinkRecord2))

      verify(documentClient).createDocument(mockEq("coll1"),
        argThat(new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument.toString == doc1.toString
          }
        }), argThat {
          new ArgumentMatcher[RequestOptions] {
            override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Eventual
          }
        }, mockEq(false))

      verify(documentClient).createDocument(mockEq("coll2"),
        argThat(new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            doc2.toString == argument.toString
          }
        }), argThat {
          new ArgumentMatcher[RequestOptions] {
            override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Eventual
          }
        }
        , mockEq(false))
    }

    "handle util.Map UPSERT with Eventual consistency level" in {
      val map = Map(
        DocumentDbConfig.DATABASE_CONFIG -> "database1",
        DocumentDbConfig.CONNECTION_CONFIG -> connection,
        DocumentDbConfig.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfig.CONSISTENCY_CONFIG -> ConsistencyLevel.Eventual.toString,
        DocumentDbConfig.KCQL_CONFIG -> "UPSERT INTO coll1 SELECT * FROM topic1 PK time"
      )

      val documentClient = mock[DocumentClient]
      val dbResource: ResourceResponse[Database] = mock[ResourceResponse[Database]]
      when(dbResource.getResource).thenReturn(mock[Database])


      val resource = mock[ResourceResponse[DocumentCollection]]
      when(resource.getResource).thenReturn(mock[DocumentCollection])

      when(documentClient.readCollection(mockEq("coll1"), any(classOf[RequestOptions])))
        .thenReturn(resource)


      when(documentClient.readDatabase(mockEq("database1"), any(classOf[RequestOptions])))
        .thenReturn(dbResource)

      val task = new DocumentDbSinkTask(s => documentClient)
      task.start(map)

      val json1 = scala.io.Source.fromFile(getClass.getResource(s"/transaction1.json").toURI.getPath).mkString
      val map1: java.util.Map[String, Any] = mapper.readValue(json1, `type`)
      val json2 = scala.io.Source.fromFile(getClass.getResource(s"/transaction2.json").toURI.getPath).mkString
      val map2: java.util.Map[String, Any] = mapper.readValue(json2, `type`)
      val sinkRecord1 = new SinkRecord("topic1", 0, null, null, null, map1, 1000)
      val sinkRecord2 = new SinkRecord("topic1", 0, null, null, null, map2, 1000)

      val doc1 = new Document(json1)
      doc1.setId(doc1.get("time").toString)
      val r1 = mock[ResourceResponse[Document]]
      when(r1.getResource).thenReturn(doc1)

      when(documentClient.upsertDocument(mockEq("coll1"), argThat {
        new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument != null && argument.toString == doc1.toString
          }
        }
      }, argThat {
        new ArgumentMatcher[RequestOptions] {
          override def matches(argument: RequestOptions): Boolean = argument.getConsistencyLevel == ConsistencyLevel.Eventual
        }
      }, mockEq(true)))
        .thenReturn(r1)

      val doc2 = new Document(json2)
      doc2.setId(doc2.get("time").toString)
      val r2 = mock[ResourceResponse[Document]]
      when(r2.getResource).thenReturn(doc2)

      when(documentClient.upsertDocument(mockEq("coll1"), argThat {
        new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument != null && argument.toString == doc2.toString
          }
        }
      }, argThat {
        new ArgumentMatcher[RequestOptions] {
          override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Eventual
        }
      }, mockEq(true)))
        .thenReturn(r2)

      task.put(Seq(sinkRecord1, sinkRecord2))

      verify(documentClient).upsertDocument(mockEq("coll1"),
        argThat(new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            argument.toString == doc1.toString
          }
        }), argThat {
          new ArgumentMatcher[RequestOptions] {
            override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Eventual
          }
        }, mockEq(true))

      verify(documentClient).upsertDocument(mockEq("coll1"),
        argThat(new ArgumentMatcher[Document] {
          override def matches(argument: Document): Boolean = {
            doc2.toString == argument.toString
          }
        }), argThat {
          new ArgumentMatcher[RequestOptions] {
            override def matches(argument: RequestOptions) = argument.getConsistencyLevel == ConsistencyLevel.Eventual
          }
        }
        , mockEq(true))
    }


  }
}



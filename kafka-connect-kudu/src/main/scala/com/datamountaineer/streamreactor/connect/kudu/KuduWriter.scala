package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.{ConnectUtils, Logging}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.kududb.client.shaded.com.google.common.primitives.Bytes
import org.kududb.client._

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
object KuduWriter {
  def apply(config: KuduSinkConfig, context: SinkTaskContext) = {
    val kuduMaster = config.getString(KuduSinkConfig.KUDU_MASTER)
    val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    new KuduWriter(client = client, context = context)
  }
}

class KuduWriter(client: KuduClient, context: SinkTaskContext) extends Logging {
  log.info("Initialising Kudu writer")
  private val topics = context.assignment().asScala.map(c=>c.topic()).toList
  private val kuduTableInserts = buildTableCache(topics)
  private val session = client.newSession()
  session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
  private val utils = new ConnectUtils

  /**
    * Build a cache of Kudu insert statements per topic
    *
    * @param topics Topic list, we are expecting pre created tables in Kudu
    * @return A Map of topic -> Insert
    **/
  private def buildTableCache(topics: List[String]): Map[String, Insert] = {
    topics.map(t => (t, client.openTable(t).newInsert())).toMap
  }

  /**
    * Write SinkRecords to Kudu
    *
    * @param records A list of SinkRecords to write
    * */
  def write(records: List[SinkRecord]) = {
    //map the rows onto the inserts in the cache
    records.map(r => convertToKudu(r))
    //apply the inserts for each topic
    topics.map(t => session.apply(kuduTableInserts.get(t).get))
  }

  /**
    * Convert a SinkRecord to a Kudu Row
    *
    * @param record A SinkRecord to convert
    * @return A Kudu Row
    * */
  private def convertToKudu(record: SinkRecord) : PartialRow = {
    val row = kuduTableInserts.get(record.topic()).get.getRow
    val fields =  record.valueSchema().fields().asScala
    fields.foreach(f => convertTypeAndAdd(f.schema().`type`(), f.name(), record, row))
    row
  }

  /**
    * Convert SinkRecord type to Kudu and add the column to the Kudu row
    *
    * @param fieldType Type of SinkRecord field
    * @param fieldName Name of SinkRecord field
    * @param record    The SinkRecord
    * @param row       The Kudu row to add the field tp
    * @return the updated Kudu row
    **/
  private def convertTypeAndAdd(fieldType: Type, fieldName: String, record: SinkRecord, row: PartialRow): PartialRow = {
    val avro = utils.convertToGenericAvro(record)
    fieldType match {
      case Type.STRING => row.addString(fieldName, avro.get(fieldName).toString)
      case Type.INT8 => row.addByte(fieldName, avro.get(fieldName).asInstanceOf[Byte])
      case Type.INT16 => row.addShort(fieldName, avro.get(fieldName).asInstanceOf[Short])
      case Type.INT32 => row.addInt(fieldName, avro.get(fieldName).asInstanceOf[Int])
      case Type.INT64 => row.addLong(fieldName, avro.get(fieldName).asInstanceOf[Long])
      case Type.BOOLEAN => row.addBoolean(fieldName, avro.get(fieldName).asInstanceOf[Boolean])
      case Type.FLOAT32 | Type.FLOAT64 => row.addFloat(fieldName, avro.get(fieldName).asInstanceOf[Float])
      case Type.BYTES => row.addBinary(fieldName, avro.get(fieldName).asInstanceOf[Array[Byte]])
      case _ => throw new UnsupportedOperationException(s"Unknown type $fieldType")
    }
    row
  }

  /**
    * Close the Kudu session and client
    * */
  def close() = {
    session.close()
    client.close()
  }
}

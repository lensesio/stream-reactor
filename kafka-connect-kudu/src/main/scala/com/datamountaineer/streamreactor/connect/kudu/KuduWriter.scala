package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.{ConnectUtils, Logging}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.kududb.client.{PartialRow, KuduClient}

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
  private val kuduRows = buildTableCache(topics)
  private val session = client.newSession()
  private val utils = new ConnectUtils

  private def buildTableCache(topics: List[String]) : Map[String, PartialRow] = {
    topics.map(t=>(t, client.openTable(t).newInsert().getRow)).toMap
  }

  def write(records: List[SinkRecord]) = {
    val kuduRecords = records.map(r=>convertToKudu(r))
  }

  private def convertToKudu(record: SinkRecord) : PartialRow = {
    val row = kuduRows.get(record.topic()).get
    val sinkRecordSchema = record.valueSchema()
    val fields =  record.valueSchema().fields().asScala
    fields.map(f=>convertTypeAndAdd(f,record, row))
    row
  }

  private def convertTypeAndAdd(field: Field, record: SinkRecord, row : PartialRow) = {
    val json: JsonNode = utils.convertValueToJson(record)

    field.schema().`type`() match {
      case Type.STRING => row.addString(field.name(), json.get(field.name()).asText())
      case Type.INT8 => row.addInt(field.name(), json.get(field.name()).asInt())
      case Type.INT16 => row.addInt(field.name(), json.get(field.name()).asInt())
      case Type.INT32 => row.addInt(field.name(), json.get(field.name()).asInt())
      case Type.INT64 => row.addInt(field.name(), json.get(field.name()).asInt())
    }
  }


  def close() = {
    session.close()
    client.close()
  }
}

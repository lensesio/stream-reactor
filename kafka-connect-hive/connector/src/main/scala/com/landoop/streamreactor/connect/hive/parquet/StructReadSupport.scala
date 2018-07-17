package com.landoop.streamreactor.connect.hive.parquet

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.kafka.connect.data.Struct
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

class StructReadSupport extends ReadSupport[Struct] {

  override def prepareForRead(configuration: Configuration,
                              metaData: util.Map[String, String],
                              fileSchema: MessageType,
                              context: ReadSupport.ReadContext): RecordMaterializer[Struct] = {
    // the file schema in here comes from the footer of the parquet file
    val schema = ParquetSchemas.toKafka(fileSchema)
    new StructMaterializer(schema)
  }

  override def init(context: InitContext): ReadSupport.ReadContext = {
    new ReadSupport.ReadContext(context.getFileSchema)
  }
}

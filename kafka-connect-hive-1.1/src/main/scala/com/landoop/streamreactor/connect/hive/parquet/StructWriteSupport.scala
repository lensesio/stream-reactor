package com.landoop.streamreactor.connect.hive.parquet

import com.landoop.streamreactor.connect.hive._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConverters._

// derived from Apache Spark's parquet write support, archive and license here:
// https://github.com/apache/spark/blob/21a7bfd5c324e6c82152229f1394f26afeae771c/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala
class StructWriteSupport(schema: Schema) extends WriteSupport[Struct] {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)
  private val schemaName = if (schema.name() == null) "schema" else schema.name()
  private val parquetSchema: MessageType = ParquetSchemas.toParquetMessage(schema, schemaName)

  private val metadata = new java.util.HashMap[String, String]()
  metadata.put("written_by", "streamreactor")

  // The Parquet `RecordConsumer` to which all structs are written
  private var consumer: RecordConsumer = _

  type ValueWriter = (Any) => Unit

  override def init(conf: Configuration): WriteSupport.WriteContext = new WriteSupport.WriteContext(parquetSchema, new java.util.HashMap[String, String])
  override def finalizeWrite(): WriteSupport.FinalizedWriteContext = new FinalizedWriteContext(metadata)
  override def prepareForWrite(consumer: RecordConsumer): Unit = this.consumer = consumer

  override def write(struct: Struct): Unit = {
    writeMessage {
      writeStructFields(struct)
    }
  }

  private def writeStructFields(struct: Struct): Unit = {
    for ((field, index) <- struct.schema.fields.asScala.zipWithIndex) {
      val value = struct.get(field)
      if (value != null) {
        val writer = valueWriter(field.schema())
        writeField(field.name, index) {
          writer(value)
        }
      }
    }
  }

  def valueWriter(schema: Schema): ValueWriter = {
    // todo perhaps introduce something like spark's SpecializedGetters
    schema.`type`() match {
      case Schema.Type.BOOLEAN => value => consumer.addBoolean(value.asInstanceOf[Boolean])
      case Schema.Type.INT8 | Schema.Type.INT16 | Schema.Type.INT32 => value => consumer.addInteger(value.toString.toInt)
      case Schema.Type.INT64 => value => consumer.addLong(value.toString.toLong)
      case Schema.Type.STRING => value => consumer.addBinary(Binary.fromReusedByteArray(value.toString.getBytes))
      case Schema.Type.FLOAT32 => value => consumer.addFloat(value.toString.toFloat)
      case Schema.Type.FLOAT64 => value => consumer.addDouble(value.toString.toDouble)
      case Schema.Type.STRUCT => value => {
        logger.debug(s"Writing nested struct")
        val struct = value.asInstanceOf[Struct]
        writeGroup {
          schema.fields.asScala
            .map { field => field -> struct.get(field) }
            .zipWithIndex.foreach { case ((field, v), k) =>
            writeField(field.name, k) {
              valueWriter(field.schema)(v)
            }
          }
        }
      }
      case _ => throw UnsupportedSchemaType(schema.`type`.toString)
    }
  }

  private def writeMessage(f: => Unit): Unit = {
    consumer.startMessage()
    f
    consumer.endMessage()
  }

  private def writeGroup(f: => Unit): Unit = {
    consumer.startGroup()
    // consumer.startMessage()
    f
    //consumer.endMessage()
    consumer.endGroup()
  }

  private def writeField(name: String, k: Int)(f: => Unit): Unit = {
    consumer.startField(name, k)
    f
    consumer.endField(name, k)
  }
}
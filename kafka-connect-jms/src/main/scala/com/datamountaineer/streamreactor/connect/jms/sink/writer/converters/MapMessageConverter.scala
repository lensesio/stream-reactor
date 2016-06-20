package com.datamountaineer.streamreactor.connect.jms.sink.writer.converters

import javax.jms.{MapMessage, Message, Session}

import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._

class MapMessageConverter extends JMSMessageConverter {
  override def convert(record: SinkRecord, session: Session): Message = {
    val msg = session.createMapMessage()
    val value = record.value()
    val schema = record.valueSchema()
    schema.`type`() match {
      case Schema.Type.STRUCT =>
        val struct = value.asInstanceOf[Struct]
        struct.schema().fields().foreach { f =>
          MapMessageBuilderFn(f.name(), struct.get(f), f.schema(), msg, session)
        }

      case _ => MapMessageBuilderFn("field", value, schema, msg, session)
    }
    msg
  }
}


object MapMessageBuilderFn {
  def apply(fieldName: String, value: AnyRef, schema: Schema, msg: MapMessage, session: Session): Unit = {
    schema.`type`() match {
      case Schema.Type.BYTES => msg.setBytes(fieldName, value.asInstanceOf[Array[Byte]])
      case Schema.Type.BOOLEAN => msg.setBoolean(fieldName, value.asInstanceOf[Boolean])
      case Schema.Type.FLOAT32 => msg.setFloat(fieldName, value.asInstanceOf[Float])
      case Schema.Type.FLOAT64 => msg.setDouble(fieldName, value.asInstanceOf[Double])
      case Schema.Type.INT8 => msg.setByte(fieldName, value.asInstanceOf[Byte])
      case Schema.Type.INT16 => msg.setShort(fieldName, value.asInstanceOf[Short])
      case Schema.Type.INT32 => msg.setInt(fieldName, value.asInstanceOf[Int])
      case Schema.Type.INT64 => msg.setLong(fieldName, value.asInstanceOf[Long])
      case Schema.Type.STRING => msg.setString(fieldName, value.asInstanceOf[String])
      case Schema.Type.MAP => msg.setObject(fieldName, value)
      case Schema.Type.ARRAY => msg.setObject(fieldName, value)
      case Schema.Type.STRUCT =>
        val nestedMsg = session.createMapMessage()
        val struct = value.asInstanceOf[Struct]
        struct.schema().fields().foreach { f =>
          MapMessageBuilderFn(f.name(), struct.get(f), f.schema(), nestedMsg, session)
        }
        msg.setObject(fieldName, nestedMsg)

    }
  }
}
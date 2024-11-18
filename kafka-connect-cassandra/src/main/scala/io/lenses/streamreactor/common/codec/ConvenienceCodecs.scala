package io.lenses.streamreactor.common.codec

import com.datastax.driver.core.exceptions.InvalidTypeException
import com.datastax.driver.core.TypeCodec
import com.datastax.driver.core.ProtocolVersion
import com.datastax.driver.core.DataType

import java.nio.ByteBuffer

object ConvenienceCodecs {

  object TimestampCodec extends TypeCodec[String](DataType.timestamp(), classOf[String]) {

    val instance: TimestampCodec.type = this

    override def parse(value: String): String =
      if (value == null || value.isEmpty || value.equalsIgnoreCase("NULL")) null
      else value

    override def format(value: String): String =
      if (value == null) "NULL"
      else value

    def serializeNoBoxing(value: Long, protocolVersion: ProtocolVersion): ByteBuffer = {
      val bb = ByteBuffer.allocate(8)
      bb.putLong(0, value)
      bb
    }

    def deserializeNoBoxing(bytes: ByteBuffer, protocolVersion: ProtocolVersion): Long =
      if (bytes == null || bytes.remaining() == 0) 0L
      else if (bytes.remaining() != 8)
        throw new InvalidTypeException(
          s"Invalid 64-bits long value, expecting 8 bytes but got ${bytes.remaining()}",
        )
      else
        bytes.getLong(bytes.position())

    override def serialize(value: String, protocolVersion: ProtocolVersion): ByteBuffer =
      if (value == null || value.isEmpty || value.equalsIgnoreCase("NULL")) null
      else serializeNoBoxing(value.toLong, protocolVersion)

    override def deserialize(bytes: ByteBuffer, protocolVersion: ProtocolVersion): String =
      if (bytes == null || bytes.remaining() == 0) null
      else deserializeNoBoxing(bytes, protocolVersion).toString
  }

  val ALL_INSTANCES = List(TimestampCodec.instance)
}

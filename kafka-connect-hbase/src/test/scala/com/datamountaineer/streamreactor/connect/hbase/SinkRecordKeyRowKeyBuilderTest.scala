package com.datamountaineer.streamreactor.connect.hbase

import com.datamountaineer.streamreactor.connect.rowkeys.{SinkRecordKeyRowKeyBuilderBytes}
import com.datamountaineer.streamreactor.connect.schemas.BytesHelper._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class SinkRecordKeyRowKeyBuilderTest extends WordSpec with Matchers with MockitoSugar {
  val keyRowKeyBuilder = new SinkRecordKeyRowKeyBuilderBytes()

  "SinkRecordKeyRowKeyBuilder" should {
    "create the right key from the Schema key value - Byte" in {
      val b = 123.toByte
      val sinkRecord = new SinkRecord("", 1, Schema.INT8_SCHEMA, b, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord, "Should not matter") shouldBe Array(b)

    }
    "create the right key from the Schema key value - String" in {
      val s = "somekey"
      val sinkRecord = new SinkRecord("", 1, Schema.STRING_SCHEMA, s, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord, Nil) shouldBe s.fromString()
    }

    "create the right key from the Schema key value - Bytes" in {
      val bArray = Array(23.toByte, 24.toByte, 242.toByte)
      val sinkRecord = new SinkRecord("", 1, Schema.BYTES_SCHEMA, bArray, Schema.FLOAT64_SCHEMA, Nil, 0)
      keyRowKeyBuilder.build(sinkRecord, Nil) shouldBe bArray
    }
    "create the right key from the Schema key value - Boolean" in {
      val bool = true
      val sinkRecord = new SinkRecord("", 1, Schema.BOOLEAN_SCHEMA, bool, Schema.FLOAT64_SCHEMA, Nil, 0)

      keyRowKeyBuilder.build(sinkRecord, Nil) shouldBe bool.fromBoolean()

    }

  }
}

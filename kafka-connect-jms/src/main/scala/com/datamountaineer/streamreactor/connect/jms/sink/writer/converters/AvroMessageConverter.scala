/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.jms.sink.writer.converters

import java.io.ByteArrayOutputStream
import javax.jms.{Message, Session}

import io.confluent.connect.avro.AvroData
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.connect.sink.SinkRecord

class AvroMessageConverter extends JMSMessageConverter {
  private lazy val avroData = new AvroData(128)

  override def convert(record: SinkRecord, session: Session): Message = {
    val avroRecord = avroData.fromConnectData(record.valueSchema(), record.value()).asInstanceOf[GenericRecord]
    val avroSchema = avroData.fromConnectSchema(record.valueSchema())

    val output = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](avroSchema)
    val encoder = EncoderFactory.get().binaryEncoder(output, null)

    writer.write(avroRecord, encoder)
    encoder.flush()
    output.flush()

    val message = session.createBytesMessage()
    message.writeBytes(output.toByteArray)
    message
  }
}

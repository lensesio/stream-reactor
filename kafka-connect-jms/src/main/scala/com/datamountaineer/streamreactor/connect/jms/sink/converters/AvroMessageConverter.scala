/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.common.schemas.ConverterUtil
import com.datamountaineer.streamreactor.common.serialization.AvroSerializer
import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import org.apache.kafka.connect.sink.SinkRecord

import java.io.ByteArrayOutputStream
import javax.jms.{BytesMessage, Session}
import scala.annotation.nowarn

@nowarn
class AvroMessageConverter extends JMSMessageConverter with ConverterUtil {

  @nowarn
  override def convert(record: SinkRecord, session: Session, setting: JMSSetting): (String, BytesMessage) = {
    val converted =  super[ConverterUtil].convert(record, setting.fields, setting.ignoreField)
    val avroRecord = convertValueToGenericAvro(converted)
    val avroSchema = avroData.fromConnectSchema(converted.valueSchema())

    implicit  val os = new ByteArrayOutputStream()
    AvroSerializer.write(avroRecord, avroSchema)

    val message = session.createBytesMessage()
    message.writeBytes(os.toByteArray)
    (setting.source, message)
  }
}

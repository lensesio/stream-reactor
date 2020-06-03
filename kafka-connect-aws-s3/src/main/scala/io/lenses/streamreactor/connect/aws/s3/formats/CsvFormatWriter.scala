
/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.formats

import java.io.OutputStreamWriter

import au.com.bytecode.opencsv.CSVWriter
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.Topic
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConverters._
import scala.util.Try

class CsvFormatWriter(outputStreamFn: () => S3OutputStream, writeHeaders: Boolean) extends S3FormatWriter with LazyLogging {

  private val outputStream: S3OutputStream = outputStreamFn()
  private val outputStreamWriter = new OutputStreamWriter(outputStream)
  private val csvWriter = new CSVWriter(outputStreamWriter)

  private var outstandingRename: Boolean = false

  private var fieldsWritten = false

  private var fields: Array[String] = _

  private val supportedPrimitiveTypes = Set(
    classOf[java.lang.Boolean],
    classOf[java.lang.Byte],
    classOf[java.lang.Double],
    classOf[java.lang.Float],
    classOf[java.lang.Integer],
    classOf[java.lang.Long],
    classOf[java.lang.Short],
    classOf[java.lang.String],
  )

  override def write(struct: Struct, topic: Topic): Unit = {

    // TODO fields
    if (!fieldsWritten) {
      writeFields(struct.schema())
    }
    val structValueLookupFn: String => String = lookupFieldValueFromStruct(struct)
    val nextRow = fields.map(structValueLookupFn)
    csvWriter.writeNext(nextRow: _*)
    csvWriter.flush()

  }

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def close: Unit = {
    Try(outstandingRename = outputStream.complete())

    Try(csvWriter.flush())
    Try(outputStream.flush())
    Try(csvWriter.close())
    Try(outputStreamWriter.close())
    Try(outputStream.close())
  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer()

  private def lookupFieldValueFromStruct(struct: Struct)(fieldName: String) = {
    Option(struct.get(fieldName)) match {
      case Some(primitiveValue) if supportedPrimitiveTypes.exists(c => c.isInstance(primitiveValue)) =>
        primitiveValue.toString
      case Some(byteArray@Array(_*)) if byteArray.forall(entry => entry.isInstanceOf[Byte]) =>
        new String(byteArray.asInstanceOf[Array[Byte]])
      case Some(other) =>
        logger.error("Non-primitive values not supported: " + other.getClass);
        throw new IllegalArgumentException("Non-primitive values not supported")
      case None =>
        null
    }
  }

  private def writeFields(schema: Schema) = {
    fields = schema.fields().asScala.map(_.name()).toArray
    if (writeHeaders) {
      csvWriter.writeNext(fields: _*)
    }
    fieldsWritten = true
  }

}

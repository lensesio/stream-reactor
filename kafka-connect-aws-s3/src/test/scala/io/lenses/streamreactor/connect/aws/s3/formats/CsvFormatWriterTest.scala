
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

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CsvFormatWriterTest extends AnyFlatSpec with Matchers {


  "convert" should "write byteoutputstream with csv for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val formatWriter = new CsvFormatWriter(() => outputStream)
    formatWriter.write(users(0), topic)

    val reader = new StringReader(new String(outputStream.toByteArray()))

    val csvReader = new CSVReader(reader)

    csvReader.readNext() should be (Array("name", "title", "salary"))
    csvReader.readNext() should be (Array("sam", "mr", "100.43"))
    csvReader.readNext() should be (null)

    csvReader.close()
    reader.close()
  }

  "convert" should "write byteoutputstream with csv for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val formatWriter = new CsvFormatWriter(() => outputStream)
    users.foreach(formatWriter.write(_, topic))

    val reader = new StringReader(new String(outputStream.toByteArray()))
    val csvReader = new CSVReader(reader)

    csvReader.readNext() should be (Array("name", "title", "salary"))
    csvReader.readNext() should be (Array("sam", "mr", "100.43"))
    csvReader.readNext() should be (Array("laura", "ms", "429.06"))
    csvReader.readNext() should be (Array("tom", "", "395.44"))
    csvReader.readNext() should be (null)

    csvReader.close()
    reader.close()

  }
}

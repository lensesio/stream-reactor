/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.common.serialization

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.RecordFormat
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class AvroSerializerTest extends AnyWordSpec with Matchers {

  implicit val recordFormat = RecordFormat[Book]
  implicit val avroSchema   = AvroSchema[Book]

  "AvroSerializer" should {
    "read and write from and to Avro" in {
      val book = Book("On Intelligence", Author("Jeff", "Hawkins", 1957), "0805078533", 273, 14.72)

      implicit val os = new ByteArrayOutputStream()
      AvroSerializer.write(book)

      implicit val is = new ByteArrayInputStream(os.toByteArray)

      val actualBook = AvroSerializer.read[Book](is)

      actualBook shouldBe book
      os.toByteArray shouldBe AvroSerializer.getBytes(book)
    }
  }

  case class Author(firstName: String, lastName: String, yearBorn: Int)

  case class Book(title: String, autor: Author, isbn: String, pages: Int, price: Double)

}

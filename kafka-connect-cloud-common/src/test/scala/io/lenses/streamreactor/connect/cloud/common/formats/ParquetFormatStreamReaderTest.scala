/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats

import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.connect.cloud.common.formats.reader.ParquetStreamReader
import org.apache.kafka.connect.data.Struct
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths
import scala.util.Try
import scala.util.Using

class ParquetFormatStreamReaderTest extends AnyFlatSpec with Matchers with EitherValues {

  "iteration" should "read parquet files" in {

    val inputStreamFn: () => Either[Throwable, InputStream] =
      () => Try(getClass.getResourceAsStream("/parquet/1.parquet")).toEither
    val streamSize: Long =
      Using.resource(inputStreamFn())(_.map(_.readAllBytes().length))(_.foreach(_.close)).value.toLong
    val target =
      ParquetStreamReader(streamSize, () => inputStreamFn())
    val list = target.value.toList
    list should have size 200
    list.head.value().asInstanceOf[Struct].getString("name") should be(
      "dbiriwtgyelferkqjmgvmakxreoPnovkObfyjSCzhsaidymngstfqgkbocypzglotuahzMojaViltqGmJpBnrIew",
    )
    list(1).value().asInstanceOf[Struct].getString("name") should be(
      "oyiirzenfdzzujavsdawjyctxvpckyqkyhzzvmaoimnywcohhSnbquwbixpeDfxttbdhupeKZolcyAjwknobmoucvwoxxytytxg",
    )
    list(199).value().asInstanceOf[Struct].getString("name") should be(
      "cfmfgbDpeklnFumaugcdcHokwtockrhsyflNqKbuwsAnXpxqzicbLzleviwhZaaIaylptfegvwFwe",
    )

  }

  "iteration" should "read really big parquet files" in {
    val bigParquetFile = "/parquet-sample-data/flights-1m.parquet"
    val inputStreamFn: () => Either[Throwable, InputStream] =
      () => Try(getClass.getResourceAsStream(bigParquetFile)).toEither
    val fileSize = getFileSize(bigParquetFile)
    val target =
      ParquetStreamReader(fileSize, () => inputStreamFn())
    target.value.size should be(1_000_000)
  }

  private def getFileSize(fileName: String): Long = {
    val filePath = Paths.get(getClass.getResource(fileName).toURI)
    Try(Files.size(filePath)).toEither.leftMap(throw _).merge
  }

}

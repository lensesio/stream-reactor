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
package io.lenses.streamreactor.connect.aws.s3.formats

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.config.ObjectMetadata
import io.lenses.streamreactor.connect.aws.s3.config.StreamReaderInput
import io.lenses.streamreactor.connect.aws.s3.formats.reader.ParquetStreamReader
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.InputStream
import java.time.Instant
import java.util.Collections
import scala.util.Try

class ParquetFormatStreamReaderTest extends AnyFlatSpec with Matchers {

  private val bucketAndPath = S3Location("myBucket", "myPath".some)

  "iteration" should "read parquet files" in {
    val inputStreamFn: () => InputStream = () => getClass.getResourceAsStream("/parquet/1.parquet")
    val streamSize = {
      val stream = inputStreamFn()
      try {
        val size = stream.readAllBytes().length
        size
      } finally {
        stream.close()
      }
    }
    val now = Instant.now()
    val input = StreamReaderInput(
      inputStreamFn(),
      bucketAndPath,
      ObjectMetadata(
        bucketAndPath.bucket,
        "myBucket:myPath",
        streamSize.toLong,
        now,
      ),
      false,
      () => Try(inputStreamFn()).toEither,
      0,
      Topic("topic"),
      Collections.emptyMap,
    )
    val target =
      ParquetStreamReader(input)
    val list = target.toList
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

}

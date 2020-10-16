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

import io.lenses.streamreactor.connect.aws.s3.model.BucketAndPath
import org.apache.kafka.connect.data.Struct
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ParquetFormatStreamReaderTest extends AnyFlatSpec with Matchers {

  private val bucketAndPath = BucketAndPath("myBucket", "myPath")

  "iteration" should "read parquet files" in {
    val inputStreamFn = () => getClass.getResourceAsStream("/parquet/1.parquet")
    val fileSizeFn = () => inputStreamFn().available().longValue()
    val target = new ParquetFormatStreamReader(inputStreamFn, fileSizeFn, bucketAndPath)
    val list = target.toList
    list should have size 200
    list(0).data.value().asInstanceOf[Struct].getString("name") should be("dbiriwtgyelferkqjmgvmakxreoPnovkObfyjSCzhsaidymngstfqgkbocypzglotuahzMojaViltqGmJpBnrIew")
    list(1).data.value().asInstanceOf[Struct].getString("name") should be("oyiirzenfdzzujavsdawjyctxvpckyqkyhzzvmaoimnywcohhSnbquwbixpeDfxttbdhupeKZolcyAjwknobmoucvwoxxytytxg")
    list(199).data.value().asInstanceOf[Struct].getString("name") should be("cfmfgbDpeklnFumaugcdcHokwtockrhsyflNqKbuwsAnXpxqzicbLzleviwhZaaIaylptfegvwFwe")
  }

}

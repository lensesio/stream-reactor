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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import io.lenses.streamreactor.connect.cloud.sink.config.PartitionFieldSplitter
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PartitionFieldSplitterTest extends AnyFlatSpecLike with Matchers {
  it should "split fields" in {
    PartitionFieldSplitter.split("_key") should be(Seq("_key"))

    PartitionFieldSplitter.split("_key.field.a") should be(Seq("_key", "field", "a"))
    PartitionFieldSplitter.split("_key.`field.a`") should be(Seq("_key", "field.a"))
    PartitionFieldSplitter.split("_value.field.a") should be(Seq("_value", "field", "a"))
    PartitionFieldSplitter.split("_value.`field.a`") should be(Seq("_value", "field.a"))
    PartitionFieldSplitter.split("_value.field.a.`b.c.d`") should be(Seq("_value", "field", "a", "b.c.d"))
    PartitionFieldSplitter.split("_value.field.a.`b.c.d`.`e.f.g`") should be(Seq("_value",
                                                                                 "field",
                                                                                 "a",
                                                                                 "b.c.d",
                                                                                 "e.f.g",
    ))

  }

}

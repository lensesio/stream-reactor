
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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.{Offset, Topic}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class CommittedFileNameTest extends AnyFlatSpecLike with Matchers {

  "unapply" should "recognise filenames in prefix/topic/927/77.json format" in {
    CommittedFileName.unapply("prefix/topic/927/77.json") should be(Some("prefix", Topic("topic"), 927, Offset(77), Json))
  }

  "unapply" should "not recognise filenames other formats" in {
    CommittedFileName.unapply("prefix/topic/927/77") should be(None)
  }

  "unapply" should "not recognise filenames for non-supported file types" in {
    CommittedFileName.unapply("prefix/topic/927/77.doc") should be(None)
  }

  "unapply" should "not recognise filenames for a long path" in {
    CommittedFileName.unapply("extra/long/prefix/topic/927/77.doc") should be(None)
  }
}


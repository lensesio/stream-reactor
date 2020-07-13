
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

import java.util.UUID

import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestConfig
import org.jclouds.blobstore.domain.Blob
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3SmokeTest extends AnyFlatSpec with Matchers with S3TestConfig {

  "smoke test" should "be able to write directly to s3 proxy" in {

    val blob: Blob = blobStoreContext.getBlobStore.blobBuilder("testblob")
      .payload("<html><body>hello world2</body></html>")
      .contentType("text/html")
      .eTag(UUID.randomUUID().toString)
      .build()

    val eTag = blobStoreContext.getBlobStore.putBlob(BucketName, blob)

    eTag should not be empty

  }
}

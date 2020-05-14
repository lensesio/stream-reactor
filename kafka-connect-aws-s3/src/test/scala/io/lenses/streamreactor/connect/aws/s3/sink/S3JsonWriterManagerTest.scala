
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
import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, BucketOptions, S3Config}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.{S3ProxyContext, S3TestConfig}
import io.lenses.streamreactor.connect.aws.s3.{BucketAndPrefix, Offset, Topic, TopicPartitionOffset}
import org.apache.kafka.connect.data.Struct
import org.jclouds.blobstore.options.ListContainerOptions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3JsonWriterManagerTest extends AnyFlatSpec with Matchers with S3TestConfig {

  import S3ProxyContext._
  import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._

  private val TopicName = "mytopic"
  private val PathPrefix = "streamReactorBackups"


  "json sink" should "write single json record" in {

    val bucketAndPrefix = BucketAndPrefix(BucketName, Some(PathPrefix))
    val config = S3Config(
      "eu-west-1",
      Identity,
      Credential,
      AuthMode.Credentials,
      bucketOptions = Set(
        BucketOptions(TopicName, bucketAndPrefix, commitPolicy = DefaultCommitPolicy(None, None, Some(1)),
          format = Json,
          fileNamingStrategy = new HierarchicalS3FileNamingStrategy(Json),
        ) // JsonS3Format
      )
    )

    val sink = S3WriterManager.from(config)
    sink.write(TopicPartitionOffset(Topic(TopicName), 1, Offset(1)), users.head)
    sink.close()


    //val list1 = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix(""))

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(1)

    readFileToString("streamReactorBackups/mytopic/1/1.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
  }

  "json sink" should "write schemas to json" in {

    val bucketAndPrefix = BucketAndPrefix(BucketName, Some(PathPrefix))
    val config = S3Config(
      "eu-west-1",
      Identity,
      Credential,
      AuthMode.Credentials,
      bucketOptions = Set(
        BucketOptions(TopicName, bucketAndPrefix, commitPolicy = DefaultCommitPolicy(None, None, Some(3)),
          format = Json,
          fileNamingStrategy = new HierarchicalS3FileNamingStrategy(Json)) // JsonS3Format
      )
    )

    val sink = S3WriterManager.from(config)
    users.zipWithIndex.foreach {
      case (struct: Struct, index: Int) => sink.write(TopicPartitionOffset(Topic(TopicName), 1, Offset(index + 1)), struct)
    }

    sink.close()

    //val list1 = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix(""))

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/mytopic/1/")).size() should be(1)

    readFileToString("streamReactorBackups/mytopic/1/3.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""")
  }

}

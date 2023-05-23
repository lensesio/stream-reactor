package io.lenses.streamreactor.connect

import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.testcontainers.S3Authentication

object Configuration {

  def sinkConfig(auth: S3Authentication, networkAliasUrl: String, bucketName: String, prefix: String, storeAs: String = "json", compressionCodec: Option[String] = Option.empty, compressionLevel: Option[Int] = Option.empty, topicName: String): ConnectorConfiguration = {

    val conn = ConnectorConfiguration
      .create
      .`with`("connector.class", "io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector")
      .`with`("tasks.max", "1")
      .`with`("topics", topicName)
      .`with`("connect.s3.aws.access.key", auth.identity)
      .`with`("connect.s3.aws.secret.key", auth.credential)
      .`with`("connect.s3.aws.auth.mode", "Credentials")
      .`with`("connect.s3.custom.endpoint", networkAliasUrl)
      .`with`("connect.s3.vhost.bucket", "true")
      .`with`("connect.s3.aws.region", "eu-west-1")
      .`with`("connect.s3.kcql", s"INSERT INTO `$bucketName:$prefix` SELECT * FROM `$topicName` STOREAS `$storeAs` WITH_FLUSH_COUNT=1")

    compressionCodec.map { s: String => conn.`with`("connect.s3.compression.codec", s) }
    compressionLevel.map { i: Int =>
      conn.`with`("connect.s3.compression.level", Integer.valueOf(i))
    }

    conn
  }
}

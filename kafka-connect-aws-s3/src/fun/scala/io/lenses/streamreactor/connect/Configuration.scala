package io.lenses.streamreactor.connect

import _root_.io.lenses.streamreactor.connect.testcontainers.S3Authentication
import _root_.io.lenses.streamreactor.connect.testcontainers.connect._

object Configuration {

  def sinkConfig(
    randomTestId:     String,
    auth:             S3Authentication,
    networkAliasUrl:  String,
    bucketName:       String,
    prefix:           String,
    storeAs:          String         = "json",
    compressionCodec: Option[String] = Option.empty,
    compressionLevel: Option[Int]    = Option.empty,
    topicName:        String,
  ): ConnectorConfiguration =
    ConnectorConfiguration(
      "connector" + randomTestId,
      Map(
        "connector.class"            -> StringCnfVal("io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector"),
        "tasks.max"                  -> IntCnfVal(1),
        "topics"                     -> StringCnfVal(topicName),
        "connect.s3.aws.access.key"  -> StringCnfVal(auth.identity),
        "connect.s3.aws.secret.key"  -> StringCnfVal(auth.credential),
        "connect.s3.aws.auth.mode"   -> StringCnfVal("Credentials"),
        "connect.s3.custom.endpoint" -> StringCnfVal(networkAliasUrl),
        "connect.s3.vhost.bucket"    -> BooleanCnfVal(true),
        "connect.s3.aws.region"      -> StringCnfVal("eu-west-1"),
        "connect.s3.kcql" -> StringCnfVal(
          s"INSERT INTO `$bucketName:$prefix` SELECT * FROM `$topicName` STOREAS `$storeAs` WITH_FLUSH_COUNT=1",
        ),
      ) ++ Seq(
        compressionCodec.map { s: String => ("connect.s3.compression.codec", StringCnfVal(s)) },
        compressionLevel.map { i: Int =>
          ("connect.s3.compression.level", IntCnfVal(Integer.valueOf(i)))
        },
      ).flatten.toMap,
    )
}

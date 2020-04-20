package io.lenses.streamreactor.connect.aws.s3.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst.{KCQL_PROP_SUFFIX, PROGRESS_ENABLED_CONST}

object S3SinkConfigSettings {

  val CONNECTOR_PREFIX = "connect.s3"

  val AWS_REGION: String = "aws.region"
  val AWS_ACCESS_KEY: String = "aws.access.key"
  val AWS_SECRET_KEY: String = "aws.secret.key"
  val AUTH_MODE: String = "aws.auth.mode"
  val CUSTOM_ENDPOINT: String = "aws.custom.endpoint"
  val ENABLE_VIRTUAL_HOST_BUCKETS: String = "aws.vhost.bucket"

  val AWS_BUCKET_NAME: String = "aws.secret.key"
  val AWS_PREFIX: String = "aws.prefix"

  val KcqlKey = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC = "Contains the Kafka Connect Query Language describing the flow from Apache Kafka topics to Apache Hive tables."
  val KCQL_DISPLAY = "KCQL commands"

  val PROGRESS_COUNTER_ENABLED: String = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

}

package io.lenses.streamreactor.connect.aws.s3.config

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object S3SinkConfigDef {

  import S3SinkConfigSettings._

  val config: ConfigDef = new ConfigDef()
    .define(
      AWS_REGION,
      Type.STRING,
      "",
      Importance.HIGH,
      "AWS region the Secrets manager is in"
    )
    .define(
      AWS_ACCESS_KEY,
      Type.STRING,
      "",
      Importance.HIGH,
      "AWS access key"
    )
    .define(
      AWS_SECRET_KEY,
      Type.PASSWORD,
      "",
      Importance.HIGH,
      "AWS password key"
    )
    .define(
      AUTH_MODE,
      Type.STRING,
      AuthMode.Credentials.toString,
      Importance.HIGH,
      "Authenticate mode, 'env', 'credentials', 'ec2' or 'eks'"
    )
    .define(
      CUSTOM_ENDPOINT,
      Type.STRING,
      AuthMode.Credentials.toString,
      Importance.LOW,
      "Custom S3-compatible endpoint (usually for testing)"
    )
    .define(
      ENABLE_VIRTUAL_HOST_BUCKETS,
      Type.BOOLEAN,
      false,
      Importance.LOW,
      "Enable virtual host buckets"
    )
    .define(KcqlKey, Type.STRING, Importance.HIGH, KCQL_DOC)

}

case class S3SinkConfigDefBuilder(props: util.Map[String, String])
  extends BaseConfig(S3SinkConfigSettings.CONNECTOR_PREFIX, S3SinkConfigDef.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings

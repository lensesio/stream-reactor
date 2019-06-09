package com.landoop.streamreactor.connect.hive.kerberos

import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfigDefBuilder
import com.landoop.streamreactor.connect.hive.sink.config.SinkConfigSettings
import org.apache.kafka.common.config.ConfigException
import org.scalatest.FunSuite
import org.scalatest.Matchers

import scala.collection.JavaConverters._

class KeytabSettingsTest extends FunSuite with Matchers with FileCreation {
  test("validate a keytab setting") {
    val file = createFile("keytab1.keytab")
    try {
      val principal = "hdfs-user@MYCORP.NET"
      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name" -> "mydatabase",
          "connect.hive.hive.metastore" -> "thrift",
          "connect.hive.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
          "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey -> "true",
          SinkConfigSettings.PrincipalKey -> principal,
          SinkConfigSettings.KerberosKeyTabKey -> file.getAbsolutePath
        ).asJava
      )

      val actualSettings = KeytabSettings.from(config, SinkConfigSettings)
      actualSettings shouldBe KeytabSettings(principal, file.getAbsolutePath, None)
    }
    finally {
      file.delete()
    }
  }

  test("throws an exception when principal is not set") {
    val file = createFile("keytab2.keytab")
    try {
      val principal = "hdfs-user@MYCORP.NET"
      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name" -> "mydatabase",
          "connect.hive.hive.metastore" -> "thrift",
          "connect.hive.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
          "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey -> "true",
          SinkConfigSettings.KerberosKeyTabKey -> file.getAbsolutePath
        ).asJava
      )

      intercept[ConfigException] {
        KeytabSettings.from(config, SinkConfigSettings)
      }
    }
    finally {
      file.delete()
    }
  }

  test("throws an exception when the keytab is not present") {
    val principal = "hdfs-user@MYCORP.NET"
    val config = HiveSinkConfigDefBuilder(
      Map(
        "connect.hive.database.name" -> "mydatabase",
        "connect.hive.hive.metastore" -> "thrift",
        "connect.hive.hive.metastore.uris" -> "thrift://localhost:9083",
        "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
        "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
        SinkConfigSettings.KerberosKey -> "true",
        SinkConfigSettings.PrincipalKey -> principal,
        SinkConfigSettings.KerberosKeyTabKey -> "does_not_exists.keytab"
      ).asJava
    )

    intercept[ConfigException] {
      KeytabSettings.from(config, SinkConfigSettings)
    }
  }
}

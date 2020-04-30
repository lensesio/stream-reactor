package com.datamountaineer.streamreactor.connect.hbase.kerberos

import com.datamountaineer.streamreactor.connect.hbase.config.{HBaseConfig, HBaseConfigConstants}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class KeytabSettingsTest extends FunSuite with Matchers with FileCreation {
  test("validate a keytab setting") {
    val file = createFile("keytab1.keytab")
    try {
      val principal = "hdfs-user@MYCORP.NET"
      val config = HBaseConfig(
        Map(
          HBaseConfigConstants.KCQL_QUERY->s"INSERT INTO someTable SELECT * FROM someTable",
          HBaseConfigConstants.COLUMN_FAMILY->"someColumnFamily",
          HBaseConfigConstants.KerberosKey -> "true",
          HBaseConfigConstants.PrincipalKey -> principal,
          HBaseConfigConstants.KerberosKeyTabKey -> file.getAbsolutePath
        ).asJava
      )

      val actualSettings = KeytabSettings.from(config, HBaseConfigConstants)
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
      val config = HBaseConfig(
        Map(
          HBaseConfigConstants.KCQL_QUERY->s"INSERT INTO someTable SELECT * FROM someTable",
          HBaseConfigConstants.COLUMN_FAMILY->"someColumnFamily",
          HBaseConfigConstants.KerberosKey -> "true",
          HBaseConfigConstants.KerberosKeyTabKey -> file.getAbsolutePath
        ).asJava
      )

      intercept[ConfigException] {
        KeytabSettings.from(config, HBaseConfigConstants)
      }
    }
    finally {
      file.delete()
    }
  }

  test("throws an exception when the keytab is not present") {
    val principal = "hdfs-user@MYCORP.NET"
    val config = HBaseConfig(
      Map(
        HBaseConfigConstants.KCQL_QUERY->s"INSERT INTO someTable SELECT * FROM someTable",
        HBaseConfigConstants.COLUMN_FAMILY->"someColumnFamily",
        HBaseConfigConstants.KerberosKey -> "true",
        HBaseConfigConstants.PrincipalKey -> principal,
        HBaseConfigConstants.KerberosKeyTabKey -> "does_not_exists.keytab"
      ).asJava
    )

    intercept[ConfigException] {
      KeytabSettings.from(config, HBaseConfigConstants)
    }
  }
}

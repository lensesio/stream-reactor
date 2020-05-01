package com.landoop.streamreactor.connect.hive.kerberos

import java.io.File

import com.landoop.streamreactor.connect.hive.sink.config.{HiveSinkConfigDefBuilder, SinkConfigSettings}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class UserPasswordSettingsTest extends AnyFunSuite with Matchers with FileCreation {
  test("validate a user-password setting") {
    val fileKrb5 = createFile(s"krb1.krb5")
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user = "yoda"
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name" -> "mydatabase",
          "connect.hive.metastore" -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
          "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey -> "true",
          SinkConfigSettings.KerberosUserKey -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key -> fileKrb5.getAbsolutePath,
          SinkConfigSettings.KerberosJaasKey -> fileJaas.getAbsolutePath,
          SinkConfigSettings.JaasEntryNameKey -> "abc"
        ).asJava
      )

      val actualSettings = UserPasswordSettings.from(config, SinkConfigSettings)
      actualSettings shouldBe UserPasswordSettings(user, password, fileKrb5.getAbsolutePath, fileJaas.getAbsolutePath, "abc", None)
    }
    finally {
      fileKrb5.delete()
      fileJaas.delete()
    }
  }

  test("raise an exception when user is null") {
    val fileKrb5 = createFile(s"krb1.krb5")
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user = null
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name" -> "mydatabase",
          "connect.hive.metastore" -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
          "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey -> "true",
          SinkConfigSettings.KerberosUserKey -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key -> fileKrb5.getAbsolutePath,
          SinkConfigSettings.KerberosJaasKey -> fileJaas.getAbsolutePath
        ).asJava
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
      }
    }
    finally {
      fileKrb5.delete()
      fileJaas.delete()
    }
  }

  test("raises an exception where password is null") {
    val fileKrb5 = createFile(s"krb1.krb5")
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user = "yoda"
      val password = null

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name" -> "mydatabase",
          "connect.hive.metastore" -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
          "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey -> "true",
          SinkConfigSettings.KerberosUserKey -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key -> fileKrb5.getAbsolutePath,
          SinkConfigSettings.KerberosJaasKey -> fileJaas.getAbsolutePath
        ).asJava
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
      }
    }
    finally {
      fileKrb5.delete()
      fileJaas.delete()
    }
  }

  test("raise an exception when there is no krb5 file set") {
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user = "yoda"
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name" -> "mydatabase",
          "connect.hive.metastore" -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
          "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey -> "true",
          SinkConfigSettings.KerberosUserKey -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosJaasKey -> fileJaas.getAbsolutePath
        ).asJava
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
      }
    }
    finally {
      fileJaas.delete()
    }
  }

  test("raises and exception when the jaas file is not set") {
    val fileKrb5 = createFile(s"krb1.krb5")
    val fileJaas = new File(s"jaas1.jaas")
    try {
      val user = "yoda"
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name" -> "mydatabase",
          "connect.hive.metastore" -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS" -> "hdfs://localhost:8020",
          "connect.hive.kcql" -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey -> "true",
          SinkConfigSettings.KerberosUserKey -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key -> fileKrb5.getAbsolutePath
        ).asJava
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
      }
    }
    finally {
      fileKrb5.delete()
    }
  }
}

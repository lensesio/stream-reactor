/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.landoop.streamreactor.connect.hive.kerberos

import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfigDefBuilder
import com.landoop.streamreactor.connect.hive.sink.config.SinkConfigSettings
import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class UserPasswordSettingsTest extends AnyFunSuite with Matchers with FileCreation {
  test("validate a user-password setting") {
    val fileKrb5 = createFile(s"krb1.krb5")
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user     = "yoda"
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name"           -> "mydatabase",
          "connect.hive.metastore"               -> "thrift",
          "connect.hive.metastore.uris"          -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"            -> "hdfs://localhost:8020",
          "connect.hive.kcql"                    -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey         -> "true",
          SinkConfigSettings.KerberosUserKey     -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key     -> fileKrb5.getAbsolutePath,
          SinkConfigSettings.KerberosJaasKey     -> fileJaas.getAbsolutePath,
          SinkConfigSettings.JaasEntryNameKey    -> "abc",
        ).asJava,
      )

      val actualSettings = UserPasswordSettings.from(config, SinkConfigSettings)
      actualSettings shouldBe UserPasswordSettings(user,
                                                   password,
                                                   fileKrb5.getAbsolutePath,
                                                   fileJaas.getAbsolutePath,
                                                   "abc",
                                                   None,
      )
    } finally {
      fileKrb5.delete()
      fileJaas.delete()
      ()
    }
  }

  test("raise an exception when user is null") {
    val fileKrb5 = createFile(s"krb1.krb5")
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user     = null
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name"           -> "mydatabase",
          "connect.hive.metastore"               -> "thrift",
          "connect.hive.metastore.uris"          -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"            -> "hdfs://localhost:8020",
          "connect.hive.kcql"                    -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey         -> "true",
          SinkConfigSettings.KerberosUserKey     -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key     -> fileKrb5.getAbsolutePath,
          SinkConfigSettings.KerberosJaasKey     -> fileJaas.getAbsolutePath,
        ).asJava,
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
      }
    } finally {
      fileKrb5.delete()
      fileJaas.delete()
      ()
    }
  }

  test("raises an exception where password is null") {
    val fileKrb5 = createFile(s"krb1.krb5")
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user     = "yoda"
      val password = null

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name"           -> "mydatabase",
          "connect.hive.metastore"               -> "thrift",
          "connect.hive.metastore.uris"          -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"            -> "hdfs://localhost:8020",
          "connect.hive.kcql"                    -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey         -> "true",
          SinkConfigSettings.KerberosUserKey     -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key     -> fileKrb5.getAbsolutePath,
          SinkConfigSettings.KerberosJaasKey     -> fileJaas.getAbsolutePath,
        ).asJava,
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
      }
    } finally {
      fileKrb5.delete()
      fileJaas.delete()
      ()
    }
  }

  test("raise an exception when there is no krb5 file set") {
    val fileJaas = createFile(s"jaas1.jaas")
    try {
      val user     = "yoda"
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name"           -> "mydatabase",
          "connect.hive.metastore"               -> "thrift",
          "connect.hive.metastore.uris"          -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"            -> "hdfs://localhost:8020",
          "connect.hive.kcql"                    -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey         -> "true",
          SinkConfigSettings.KerberosUserKey     -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosJaasKey     -> fileJaas.getAbsolutePath,
        ).asJava,
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
      }
    } finally {
      fileJaas.delete()
      ()
    }
  }

  test("raises and exception when the jaas file is not set") {
    val fileKrb5 = createFile(s"krb1.krb5")
    try {
      val user     = "yoda"
      val password = "123456"

      val config = HiveSinkConfigDefBuilder(
        Map(
          "connect.hive.database.name"           -> "mydatabase",
          "connect.hive.metastore"               -> "thrift",
          "connect.hive.metastore.uris"          -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"            -> "hdfs://localhost:8020",
          "connect.hive.kcql"                    -> "insert into mytable select a,b,c from mytopic",
          SinkConfigSettings.KerberosKey         -> "true",
          SinkConfigSettings.KerberosUserKey     -> user,
          SinkConfigSettings.KerberosPasswordKey -> password,
          SinkConfigSettings.KerberosKrb5Key     -> fileKrb5.getAbsolutePath,
        ).asJava,
      )

      intercept[ConfigException] {
        UserPasswordSettings.from(config, SinkConfigSettings)
        ()
      }
    } finally {
      fileKrb5.delete()
      ()
    }
  }
}

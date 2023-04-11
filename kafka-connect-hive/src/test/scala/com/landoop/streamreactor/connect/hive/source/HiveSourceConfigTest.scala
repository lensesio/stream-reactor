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
package com.landoop.streamreactor.connect.hive.source

import com.landoop.streamreactor.connect.hive.source.config.HiveSourceConfig
import com.landoop.streamreactor.connect.hive.source.config.ProjectionField
import com.landoop.streamreactor.connect.hive.TableName
import com.landoop.streamreactor.connect.hive.Topic
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HiveSourceConfigTest extends AnyWordSpec with Matchers {

  "HiveSource" should {
    "populate required table properties from KCQL" in {
      val config = HiveSourceConfig.fromProps(
        Map(
          "connect.hive.database.name"  -> "mydatabase",
          "connect.hive.metastore"      -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"   -> "hdfs://localhost:8020",
          "connect.hive.kcql"           -> "insert into mytopic select a,b from mytable",
        ),
      )
      val tableConfig = config.tableOptions.head
      tableConfig.topic shouldBe Topic("mytopic")
      tableConfig.tableName shouldBe TableName("mytable")
      tableConfig.projection.get.toList shouldBe Seq(ProjectionField("a", "a"), ProjectionField("b", "b"))
    }
    "populate aliases from KCQL" in {
      val config = HiveSourceConfig.fromProps(
        Map(
          "connect.hive.database.name"  -> "mydatabase",
          "connect.hive.metastore"      -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"   -> "hdfs://localhost:8020",
          "connect.hive.kcql"           -> "insert into mytopic select a as x,b from mytable",
        ),
      )
      val tableConfig = config.tableOptions.head
      tableConfig.projection.get.toList shouldBe Seq(ProjectionField("a", "x"), ProjectionField("b", "b"))
    }
    "set projection to None for *" in {
      val config = HiveSourceConfig.fromProps(
        Map(
          "connect.hive.database.name"  -> "mydatabase",
          "connect.hive.metastore"      -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"   -> "hdfs://localhost:8020",
          "connect.hive.kcql"           -> "insert into mytopic select * from mytable",
        ),
      )
      val tableConfig = config.tableOptions.head
      tableConfig.projection shouldBe None
    }
    "set table limit" in {
      val config = HiveSourceConfig.fromProps(
        Map(
          "connect.hive.database.name"  -> "mydatabase",
          "connect.hive.metastore"      -> "thrift",
          "connect.hive.metastore.uris" -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"   -> "hdfs://localhost:8020",
          "connect.hive.kcql"           -> "insert into mytopic select a from mytable limit 200",
        ),
      )
      val tableConfig = config.tableOptions.head
      tableConfig.limit shouldBe 200
    }
    "populate refresh frequency" in {
      val config = HiveSourceConfig.fromProps(
        Map(
          "connect.hive.database.name"     -> "mydatabase",
          "connect.hive.metastore"         -> "thrift",
          "connect.hive.metastore.uris"    -> "thrift://localhost:9083",
          "connect.hive.fs.defaultFS"      -> "hdfs://localhost:8020",
          "connect.hive.kcql"              -> "insert into mytopic select a,b from mytable",
          "connect.hive.refresh.frequency" -> "100",
        ),
      )
      config.refreshFrequency should be(100)
    }
  }
}

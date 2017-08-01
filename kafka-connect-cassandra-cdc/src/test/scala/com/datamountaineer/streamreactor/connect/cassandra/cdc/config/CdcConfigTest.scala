package com.datamountaineer.streamreactor.connect.cassandra.cdc.config

import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class CdcConfigTest extends WordSpec with Matchers {
  "CdcConfig" should {
    "load basic configuration" in {
      val map: Map[String, String] = Map(
        CassandraConnect.CONTACT_POINTS -> "127.0.0.1",
        CassandraConnect.PORT -> "9042",
        CassandraConnect.KCQL -> "INSERT INTO topicA SELECT * FROM datamountaineer.orders",
        CassandraConnect.YAML_PATH -> getClass.getResource("/cassandra.yaml").getFile
      )
      val connectConfig = CassandraConnect(map)
      val cdc = CdcConfig(connectConfig)
      cdc.subscriptions.size shouldBe 1
      cdc.subscriptions shouldBe Seq(CdcSubscription("datamountaineer", "orders", "topicA"))
      cdc.decimalScale shouldBe CassandraConnect.DECIMAL_SCALE_DEFAULT
      cdc.singleInstancePort shouldBe CassandraConnect.SINGLE_INSTANCE_PORT_DEFAULT
      cdc.enableCdcFileDeleteDuringRead shouldBe CassandraConnect.ENABLE_FILE_DELETE_WHILE_READING_DEFAULT
      cdc.mutationsBufferSize shouldBe CassandraConnect.MUTATION_BUFFER_SIZE_DEFAULT

    }

    "load a configuration when the Cassandra namespace is not specified - defaults to default" in {
      val map: Map[String, String] = Map(
        CassandraConnect.CONTACT_POINTS -> "127.0.0.1",
        CassandraConnect.PORT -> "9042",
        CassandraConnect.KCQL -> "INSERT INTO topicA SELECT * FROM orders",
        CassandraConnect.YAML_PATH -> getClass.getResource("/cassandra.yaml").getFile
      )
      val connectConfig = CassandraConnect(map)
      val cdc = CdcConfig(connectConfig)
      cdc.subscriptions.size shouldBe 1
      cdc.subscriptions shouldBe Seq(CdcSubscription(null, "orders", "topicA"))
    }

    "load the configuration with DECIMAL scale set to 12" in {
      val map: Map[String, String] = Map(
        CassandraConnect.CONTACT_POINTS -> "127.0.0.1",
        CassandraConnect.PORT -> "9042",
        CassandraConnect.KCQL -> "INSERT INTO topicA SELECT * FROM datamountaineer.orders",
        CassandraConnect.YAML_PATH -> getClass.getResource("/cassandra.yaml").getFile,
        CassandraConnect.DECIMAL_SCALE -> "12"
      )
      val connectConfig = CassandraConnect(map)
      val cdc = CdcConfig(connectConfig)
      cdc.subscriptions.size shouldBe 1
      cdc.subscriptions shouldBe Seq(CdcSubscription("datamountaineer", "orders", "topicA"))
      cdc.decimalScale shouldBe 12
    }

    "load a configuration with the single instance port set to 10000" in {
      val map: Map[String, String] = Map(
        CassandraConnect.CONTACT_POINTS -> "127.0.0.1",
        CassandraConnect.PORT -> "9042",
        CassandraConnect.KCQL -> "INSERT INTO topicA SELECT * FROM datamountaineer.orders",
        CassandraConnect.YAML_PATH -> getClass.getResource("/cassandra.yaml").getFile,
        CassandraConnect.SINGLE_INSTANCE_PORT -> "10000"
      )
      val connectConfig = CassandraConnect(map)
      val cdc = CdcConfig(connectConfig)
      cdc.subscriptions.size shouldBe 1
      cdc.subscriptions shouldBe Seq(CdcSubscription("datamountaineer", "orders", "topicA"))
      cdc.decimalScale shouldBe CassandraConnect.DECIMAL_SCALE_DEFAULT
      cdc.singleInstancePort shouldBe 10000
    }

    "load a configuration with Delete files while reading disabled" in {
      val map: Map[String, String] = Map(
        CassandraConnect.CONTACT_POINTS -> "127.0.0.1",
        CassandraConnect.PORT -> "9042",
        CassandraConnect.KCQL -> "INSERT INTO topicA SELECT * FROM datamountaineer.orders",
        CassandraConnect.YAML_PATH -> getClass.getResource("/cassandra.yaml").getFile,
        CassandraConnect.ENABLE_FILE_DELETE_WHILE_READING -> "false"
      )
      val connectConfig = CassandraConnect(map)
      val cdc = CdcConfig(connectConfig)
      cdc.subscriptions.size shouldBe 1
      cdc.subscriptions shouldBe Seq(CdcSubscription("datamountaineer", "orders", "topicA"))
      cdc.decimalScale shouldBe CassandraConnect.DECIMAL_SCALE_DEFAULT
      cdc.enableCdcFileDeleteDuringRead shouldBe false
    }

    "load a configuration with Cassandra Mutation queue set to 1000000" in {
      val map: Map[String, String] = Map(
        CassandraConnect.CONTACT_POINTS -> "127.0.0.1",
        CassandraConnect.PORT -> "9042",
        CassandraConnect.KCQL -> "INSERT INTO topicA SELECT * FROM datamountaineer.orders",
        CassandraConnect.YAML_PATH -> getClass.getResource("/cassandra.yaml").getFile,
        CassandraConnect.MUTATION_BUFFER_SIZE -> "1000000"
      )
      val connectConfig = CassandraConnect(map)
      val cdc = CdcConfig(connectConfig)
      cdc.subscriptions.size shouldBe 1
      cdc.subscriptions shouldBe Seq(CdcSubscription("datamountaineer", "orders", "topicA"))
      cdc.decimalScale shouldBe CassandraConnect.DECIMAL_SCALE_DEFAULT
      cdc.mutationsBufferSize shouldBe 1000000

    }
  }
}

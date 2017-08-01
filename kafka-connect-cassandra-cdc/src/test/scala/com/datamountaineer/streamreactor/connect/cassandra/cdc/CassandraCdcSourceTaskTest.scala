package com.datamountaineer.streamreactor.connect.cassandra.cdc

import java.io.File
import java.nio.file.{Files, Paths}

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CassandraConnect
import org.apache.cassandra.config.DatabaseDescriptor
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConversions._


class CassandraCdcSourceTaskTest extends WordSpec with Matchers with BeforeAndAfterAll {

  private val testFolder = new File("cassandracdc")
  private val yamlPath = Paths.get(testFolder.getAbsolutePath, "cassandra.yaml")

  private def createFolder(folderName: String): Unit = {
    val folder = Paths.get(testFolder.getAbsolutePath, folderName).toFile
    if (!folder.exists()) {
      folder.mkdir()
    }
  }

  override def beforeAll(): Unit = {
    if (!testFolder.exists()) {
      testFolder.mkdir()
    }

    val yamlFile = yamlPath.toFile
    if (yamlFile.exists()) {
      yamlFile.delete()
    }
    Files.copy(new File(getClass.getResource("/cassandra.yaml").getFile).toPath, yamlPath)

    createFolder("commitlog")
    createFolder("data")
    createFolder("cdc_raw")

    //val fileUri = new File(getClass.getResource("/cassandra.yaml").getFile).getAbsolutePath
    System.setProperty("cassandra.config", "file:" + yamlPath)
    System.setProperty("cassandra-foreground", "true")
    System.setProperty("cassandra.native.epoll.enabled", "false")
    DatabaseDescriptor.daemonInitialization()

    EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml", testFolder.getAbsolutePath, 25000)

  }

  override def afterAll(): Unit = {

    //to avoid exception during cleanup
    //the cassandra unit seems to miss some cleanup code (isnnot shutting down the daemon; if we do below it throws some exceptions)
    DatabaseDescriptor.getRawConfig.commitlog_total_space_in_mb = 8192
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()

    /*EmbeddedCassandraServerHelper.getSession.close()
    EmbeddedCassandraServerHelper.getCluster.close()

    val field = classOf[EmbeddedCassandraServerHelper].getDeclaredField("cassandraDaemon")
    field.setAccessible(true)
    val daemon = field.get(null).asInstanceOf[CassandraDaemon]
    if (daemon != null) {
      Try(daemon.deactivate())
    }
    new File("target").delete()*/
  }

  "CassandraCdcSourceTask" should {
    "run only one instance per machine" in {
      val map: Map[String, String] = Map(
        CassandraConnect.CONTACT_POINTS -> "127.0.0.1",
        CassandraConnect.PORT -> "9042",
        CassandraConnect.KCQL -> "INSERT INTO topicA SELECT * FROM datamountaineer.orders",
        CassandraConnect.YAML_PATH -> getClass.getResource("/cassandra.yaml").getFile
      )
      val connectConfig = CassandraConnect(map)


      val task1 = new CassandraCdcSourceTask()
      val task2 = new CassandraCdcSourceTask()

      task1.start(map)
      task2.start(map)

      try {
        task1.isActuallyRunning shouldBe true
        task2.isActuallyRunning shouldBe false
      }
      finally {
        task1.stop()
        task2.stop()
      }
    }
  }
}

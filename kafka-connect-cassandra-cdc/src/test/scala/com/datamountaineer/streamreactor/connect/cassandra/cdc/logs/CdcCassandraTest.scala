package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import java.io.File
import java.nio.file.{Files, Paths}
import java.util

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.{CassandraConfig, CdcConfig, CdcSubscription}
import com.datamountaineer.streamreactor.connect.cassandra.cdc.logs.CommitLogSegmentManagerCDCExtensions._
import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.ConnectSchemaBuilder
import com.datastax.driver.core.Session
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.db.Keyspace
import org.apache.cassandra.db.commitlog.{CommitLog, CommitLogSegmentManagerCDC}
import org.apache.kafka.connect.data.Struct
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConversions._

class CdcCassandraTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val testFolder = new File("cassandracdc")

  //System.setProperty("cassandra.storagedir", Paths.get("kafka-connect-cassandra-cdc", "target", "embeddedCassandra", "data").toString)
  private val yamlPath = Paths.get(testFolder.getAbsolutePath, "cassandra.yaml")
  private val cdcFolder = Paths.get(testFolder.getAbsolutePath, "cdc_raw")

  private var session: Session = _

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


    //fix for cassandra-unit
    DatabaseDescriptor.daemonInitialization()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml", 25000)
    DatabaseDescriptor.isCDCEnabled shouldBe true

    session = EmbeddedCassandraServerHelper.getSession
  }

  override def afterAll(): Unit = {

    //to avoid exception during cleanup
    //the cassandra unit seems to miss some cleanup code (isnnot shutting down the daemon; if we do below it throws some exceptions)
    //DatabaseDescriptor.getRawConfig.commitlog_total_space_in_mb = 8192
    //EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()

    /* CommitLog.instance.shutdownBlocking()

     val field = classOf[EmbeddedCassandraServerHelper].getDeclaredField("cassandraDaemon")
     field.setAccessible(true)
     val daemon = field.get(null).asInstanceOf[CassandraDaemon]
     if (daemon != null) {
       daemon.thriftServer.stop()
       daemon.stopNativeTransport()
       StorageService.instance.setRpcReady(false)
        //Try(daemon.stop())
       //Try(daemon.destroy())
     }
     new File("target").delete()*/

    //new File("target/embeddedCassandra").delete()
  }

  "CdcCassandra" should {
    "create table datamountaineer1.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) and insert 3 records" in {

      val keyspace = "datamountaineer1"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""create table $keyspace.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created))
           |WITH CLUSTERING ORDER BY (created asc);
        """.stripMargin)


      session.execute(s"ALTER TABLE $keyspace.orders WITH cdc=true;")
      session.execute(s"INSERT INTO $keyspace.orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);")
      session.execute(s"INSERT INTO $keyspace.orders (id, created, product, qty, price) VALUES (2, now(), 'OP-DAX-C-20150201-100', 100, 99.5);")
      session.execute(s"INSERT INTO $keyspace.orders (id, created, product, qty, price) VALUES (3, now(), 'FU-KOSPI-C-20150201-100', 200, 150);")

      val cdc = createCdcAndFlush(keyspace, "orders")

      Thread.sleep(2000)
      var mutations = cdc.getMutations()
      /* if (mutations.isEmpty) {
         Thread.sleep(2000)
         mutations = cdc.getMutations()
       }*/
      mutations.size shouldBe 3

      val m1 = mutations.get(0)
      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 2
      tableKeys.get("id") shouldBe 1


      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value1.schema().fields().map(_.name()).toSet shouldBe Set("id", "created", "product", "qty", "price")
      value1.get("id") shouldBe 1
      value1.get("created") != null shouldBe true
      value1.get("product") shouldBe "OP-DAX-P-20150201-95.7"
      value1.get("qty") shouldBe 100
      value1.get("price") shouldBe 94.2f


      val m2 = mutations.get(1)
      val key2 = m2.key().asInstanceOf[Struct]
      key2.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key2.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys2 = key2.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys2.schema().fields().size() shouldBe 2
      tableKeys2.get("id") shouldBe 2

      cdcStruct = m2.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value2 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value2.schema().fields().map(_.name()).toSet shouldBe Set("id", "created", "product", "qty", "price")
      value2.get("id") shouldBe 2
      value2.get("created") != null shouldBe true
      value2.get("product") shouldBe "OP-DAX-C-20150201-100"
      value2.get("qty") shouldBe 100
      value2.get("price") shouldBe 99.5f


      val m3 = mutations.get(2)
      val key3 = m3.key().asInstanceOf[Struct]
      key3.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key3.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys3 = key3.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys3.schema().fields().size() shouldBe 2
      tableKeys3.get("id") shouldBe 3

      cdcStruct = m3.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value3 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value3.schema().fields().map(_.name()).toSet shouldBe Set("id", "created", "product", "qty", "price")
      value3.get("id") shouldBe 3
      value3.get("created") != null shouldBe true
      value3.get("product") shouldBe "FU-KOSPI-C-20150201-100"
      value3.get("qty") shouldBe 200
      value3.get("price") shouldBe 150f

      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)

      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0

      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }

    "create table datamountaineer.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) and insert 1 record and delete it" in {

      val keyspace = "datamountaineer2"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""create table $keyspace.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created))
           |WITH CLUSTERING ORDER BY (created asc);
        """.stripMargin)


      session.execute(s"ALTER TABLE $keyspace.orders WITH cdc=true;")
      session.execute(s"INSERT INTO $keyspace.orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);")
      session.execute(s"DELETE FROM $keyspace.orders where id = 1")

      val cdc = createCdcAndFlush(keyspace, "orders")

      Thread.sleep(3000)
      var mutations = cdc.getMutations()

      mutations.size shouldBe 2

      val m1 = mutations.get(0)
      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 2
      tableKeys.get("id") shouldBe 1

      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value1.schema().fields().map(_.name()).toSet shouldBe Set("id", "created", "product", "qty", "price")
      value1.get("id") shouldBe 1
      value1.get("created") != null shouldBe true
      value1.get("product") shouldBe "OP-DAX-P-20150201-95.7"
      value1.get("qty") shouldBe 100
      value1.get("price") shouldBe 94.2f


      val m2 = mutations.get(1)
      val key2 = m2.key().asInstanceOf[Struct]
      key2.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key2.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys2 = key2.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys2.schema().fields().size() shouldBe 2
      tableKeys2.get("id") shouldBe 1

      cdcStruct = m2.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.DELETE.toString

      val value2 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value2 eq null shouldBe false
      value2.get("id") shouldBe 1

      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)
      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0
      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }

    "create table datamountaineer3.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) and insert 1 record and delete one column" in {

      val keyspace = "datamountaineer3"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""create table $keyspace.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created))
           |WITH CLUSTERING ORDER BY (created asc);
        """.stripMargin)


      session.execute(s"ALTER TABLE $keyspace.orders WITH cdc=true;")
      session.execute(s"INSERT INTO $keyspace.orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);")
      val rs = session.execute(s"SELECT created FROM $keyspace.orders where id = 1")
      val created = rs.head.getObject(0).toString
      session.execute(s"DELETE product FROM $keyspace.orders where id = 1 and created=$created")


      val cdc = createCdcAndFlush(keyspace, "orders")

      Thread.sleep(4000)
      var mutations = cdc.getMutations()

      mutations.size shouldBe 2

      val m1 = mutations.get(0)

      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 2
      tableKeys.get("id") shouldBe 1

      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value1.schema().fields().map(_.name()).toSet shouldBe Set("id", "created", "product", "qty", "price")
      value1.get("id") shouldBe 1
      value1.get("created") != null shouldBe true
      value1.get("product") shouldBe "OP-DAX-P-20150201-95.7"
      value1.get("qty") shouldBe 100
      value1.get("price") shouldBe 94.2f


      val m2 = mutations.get(1)
      val key2 = m2.key().asInstanceOf[Struct]
      key2.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key2.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys2 = key2.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys2.schema().fields().size() shouldBe 2
      tableKeys2.get("id") shouldBe 1

      cdcStruct = m2.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.DELETE_COLUMN.toString
      metadataStruct.get(ConnectSchemaBuilder.DeletedColumnsField) == null shouldBe false
      metadataStruct.get(ConnectSchemaBuilder.DeletedColumnsField) shouldBe new util.ArrayList[String](Seq("product"))

      val value2 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]

      value2.get("id") shouldBe 1
      value2.get("created") eq null shouldBe false
      value2.get("product") eq null shouldBe true
      value2.get("qty") eq null shouldBe true
      value2.get("price") eq null shouldBe true


      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)
      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0
      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }

    "create table datamountaineer4.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) and insert 1 record and update the one column" in {

      val session = EmbeddedCassandraServerHelper.getSession
      val keyspace = "datamountaineer4"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""create table $keyspace.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created))
           |WITH CLUSTERING ORDER BY (created asc);
        """.stripMargin)


      session.execute(s"ALTER TABLE $keyspace.orders WITH cdc=true;")
      session.execute(s"INSERT INTO $keyspace.orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);")
      val rs = session.execute(s"SELECT created FROM $keyspace.orders where id = 1")
      val created = rs.head.getObject(0).toString
      session.execute(s"UPDATE $keyspace.orders SET product='abc', price=1000.111 where id = 1 and created=$created")


      val cdc = createCdcAndFlush(keyspace, "orders")

      Thread.sleep(4000)
      var mutations = cdc.getMutations()

      mutations.size shouldBe 2

      val m1 = mutations.get(0)
      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 2
      tableKeys.get("id") shouldBe 1

      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value1.schema().fields().map(_.name()).toSet shouldBe Set("id", "created", "product", "qty", "price")
      value1.get("id") shouldBe 1
      value1.get("created") != null shouldBe true
      value1.get("product") shouldBe "OP-DAX-P-20150201-95.7"
      value1.get("qty") shouldBe 100
      value1.get("price") shouldBe 94.2f


      val m2 = mutations.get(1)
      val key2 = m2.key().asInstanceOf[Struct]
      key2.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key2.get(ConnectSchemaBuilder.TableField) shouldBe "orders"
      var tableKeys2 = key2.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys2.schema().fields().size() shouldBe 2
      tableKeys2.get("id") shouldBe 1

      cdcStruct = m2.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString


      val value2 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value2.get("id") shouldBe 1
      value2.get("created") shouldBe created
      value2.get("product") shouldBe "abc"
      value2.get("price") shouldBe 1000.111f


      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)
      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0
      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }


    "create table datamountaineer5.user (user_id text PRIMARY KEY,name text,rlist list<text>) and insert 1 record" in {

      val keyspace = "datamountaineer5"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""
           |create table $keyspace.user (user_id text PRIMARY KEY,name text,rlist list<text>);""".stripMargin)


      session.execute(s"ALTER TABLE $keyspace.user WITH cdc=true;")
      session.execute(s"INSERT INTO $keyspace.user (user_id, name, rlist) VALUES ('stef', 'stefan', [ 'SAN', 'UK', 'LON']);")


      val cdc = createCdcAndFlush(keyspace, "user")

      Thread.sleep(4000)
      var mutations = cdc.getMutations()

      mutations.size shouldBe 1

      val m1 = mutations.get(0)
      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe "user"
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 1
      tableKeys.get("user_id") shouldBe "stef"

      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value1.schema().fields().map(_.name()).toSet shouldBe Set("user_id", "name", "rlist")
      value1.get("user_id") shouldBe "stef"
      value1.get("name") shouldBe "stefan"
      value1.get("rlist") shouldBe new util.ArrayList[String](Seq("SAN", "UK", "LON"))

      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)
      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0
      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }

    "create table datamountaineer6.course ( id text PRIMARY KEY, prereq map<int, text> ) and insert 1 record" in {

      val keyspace = "datamountaineer6"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""
           |create table $keyspace.course  ( id text PRIMARY KEY, prereq map<int, text> );""".stripMargin)


      session.execute(s"ALTER TABLE $keyspace.course WITH cdc=true;")
      session.execute(s"INSERT INTO $keyspace.course (id, prereq) VALUES ('id1', { 100:'value1', 101:'value2'});")

      val cdc = createCdcAndFlush(keyspace, "course")

      Thread.sleep(4000)
      var mutations = cdc.getMutations()

      mutations.size shouldBe 1

      val m1 = mutations.get(0)
      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe "course"
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 1
      tableKeys.get("id") shouldBe "id1"

      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value1.schema().fields().map(_.name()).toSet shouldBe Set("id", "prereq")
      value1.get("id") shouldBe "id1"

      val prereqMap = new util.HashMap[Int, String]()
      prereqMap.put(100, "value1")
      prereqMap.put(101, "value2")

      value1.get("prereq") shouldBe prereqMap

      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)
      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0
      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }

    "create table datamountaineer7.tblset ( id text PRIMARY KEY, something set<int> ) and insert 1 record" in {

      val keyspace = "datamountaineer7"
      val table = "tblset"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""
           |create table $keyspace.$table ( id text PRIMARY KEY, something set<int> );""".stripMargin)


      session.execute(s"ALTER TABLE $keyspace.$table WITH cdc=true;")
      session.execute(s"INSERT INTO $keyspace.$table (id, something) VALUES ('id1', {1,2,3,4,1,2});")

      val cdc = createCdcAndFlush(keyspace, table)

      Thread.sleep(4000)
      var mutations = cdc.getMutations()

      mutations.size shouldBe 1

      val m1 = mutations.get(0)
      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe table
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 1
      tableKeys.get("id") shouldBe "id1"

      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value1.schema().fields().map(_.name()).toSet shouldBe Set("id", "something")
      value1.get("id") shouldBe "id1"

      val expected = new util.HashSet[Int](Seq(1, 2, 3, 4))

      val actual = value1.get("something").asInstanceOf[util.ArrayList[Object]].foldLeft(new util.HashSet[Int]()) { case (s, v) =>
        s.add(v.asInstanceOf[Int])
        s
      }
      actual shouldBe expected

      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)
      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0
      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }


    "create table datamountaineer8.users ( id uuid PRIMARY KEY, name frozen <fullname>, direct_reports set<frozen <fullname>>,  addresses map<text, frozen <address>>);   and insert 1 record" in {

      val keyspace = "datamountaineer8"
      val table = "users"
      session.execute(s"CREATE KEYSPACE $keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};")

      session.execute(
        s"""
           |CREATE TYPE $keyspace.address(
           |  street text,
           |  city text,
           |  zip_code int,
           |  phones set<text>
           |);
        """.stripMargin)

      session.execute(
        s"""
           |CREATE TYPE $keyspace.fullname (
           |  firstname text,
           |  lastname text
           |);
        """.stripMargin
      )
      session.execute(
        s"""
           |CREATE TABLE $keyspace.$table(
           |id uuid PRIMARY KEY,
           |name  fullname,
           |direct_reports set<frozen <fullname>>,
           |other_reports list<frozen <fullname>>,
           |addresses map<text, frozen <address>>);
           |""".stripMargin)


      session.execute(s"ALTER TABLE $keyspace.$table WITH cdc=true;")
      session.execute(
        s"""
           |INSERT INTO $keyspace.$table(id, name) VALUES (62c36092-82a1-3a00-93d1-46196ee77204,{firstname:'Marie-Claude',lastname:'Josset'});
         """.stripMargin)


      session.execute(
        s"""
           |UPDATE $keyspace.$table
           |SET
           | addresses = addresses +
           |   {
           |    'home': {
           |        street: '191 Rue St. Charles',
           |        city: 'Paris',
           |        zip_code: 75015,
           |        phones: {'33 6 78 90 12 34'}
           |     },
           |     'work': {
           |        street: '81 Rue de Paradis',
           |        city: 'Paris',
           |        zip_code: 7500,
           |        phones: {'33 7 12 99 11 00'}
           |     }
           |   }
           |WHERE id=62c36092-82a1-3a00-93d1-46196ee77204;
        """.stripMargin)

      session.execute(
        s"""
           |INSERT INTO $keyspace.$table(id, direct_reports) VALUES (11c11111-82a1-3a00-93d1-46196ee77204,{{firstname:'Jean-Claude',lastname:'Van Damme'}, {firstname:'Arnold', lastname:'Schwarzenegger'}});
         """.stripMargin)

      session.execute(
        s"""
           |INSERT INTO $keyspace.$table(id, other_reports) VALUES (22c11111-82a1-3a00-93d1-46196ee77204,[{firstname:'Jean-Claude',lastname:'Van Damme'}, {firstname:'Arnold', lastname:'Schwarzenegger'}]);
         """.stripMargin)

      session.execute(
        s"""
           |DELETE name.firstname FROm $keyspace.$table WHERE id=62c36092-82a1-3a00-93d1-46196ee77204;
         """.stripMargin)


      val cdc = createCdcAndFlush(keyspace, table)

      Thread.sleep(4000)
      var mutations = cdc.getMutations()

      mutations.size shouldBe 5

      val m1 = mutations.get(0)
      val key1 = m1.key().asInstanceOf[Struct]
      key1.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key1.get(ConnectSchemaBuilder.TableField) shouldBe table
      var tableKeys = key1.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys.schema().fields().size() shouldBe 1
      tableKeys.get("id") shouldBe "62c36092-82a1-3a00-93d1-46196ee77204"

      var cdcStruct = m1.value().asInstanceOf[Struct]
      var metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value1 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      val fields = value1.schema().fields().map(_.name()).toVector.sortBy(identity)
      fields shouldBe Vector("id", "name", "addresses", "other_reports", "direct_reports").sortBy(identity)
      value1.get("id") shouldBe "62c36092-82a1-3a00-93d1-46196ee77204"

      value1.get("addresses") == null shouldBe true
      value1.get("direct_reports") == null shouldBe true

      val nameStruct = value1.get("name").asInstanceOf[Struct]
      nameStruct == null shouldBe false
      nameStruct.schema().fields().map(_.name()).toSet shouldBe Set("firstname", "lastname")
      nameStruct.get("firstname") shouldBe "Marie-Claude"
      nameStruct.get("lastname") shouldBe "Josset"


      val m2 = mutations.get(1)
      val key2 = m2.key().asInstanceOf[Struct]
      key2.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key2.get(ConnectSchemaBuilder.TableField) shouldBe table
      var tableKeys2 = key2.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys2.schema().fields().size() shouldBe 1
      tableKeys2.get("id") shouldBe "62c36092-82a1-3a00-93d1-46196ee77204"

      cdcStruct = m2.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value2 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value2.schema().fields().map(_.name()).toVector.sortBy(identity) shouldBe Vector("id", "name", "addresses", "other_reports", "direct_reports").sortBy(identity)
      value2.get("id") shouldBe "62c36092-82a1-3a00-93d1-46196ee77204"

      value2.get("name") == null shouldBe true
      value2.get("direct_reports") == null shouldBe true

      val addresses = value2.get("addresses")
      addresses == null shouldBe false
      val addressMap = addresses.asInstanceOf[util.HashMap[String, Struct]]
      addressMap.containsKey("home") shouldBe true
      val homeStruct = addressMap.get("home")
      homeStruct.get("street") shouldBe "191 Rue St. Charles"
      homeStruct.get("city") shouldBe "Paris"
      homeStruct.get("zip_code") shouldBe 75015
      homeStruct.get("phones") == null shouldBe false
      val phonesHome = homeStruct.get("phones").asInstanceOf[util.ArrayList[_]]
      phonesHome.contains("33 6 78 90 12 34")
      phonesHome.size() shouldBe 1

      addressMap.containsKey("work") shouldBe true
      val workStruct = addressMap.get("work")
      workStruct.get("street") shouldBe "81 Rue de Paradis"
      workStruct.get("city") shouldBe "Paris"
      workStruct.get("zip_code") shouldBe 7500
      workStruct.get("phones") == null shouldBe false
      val phonesWork = workStruct.get("phones").asInstanceOf[util.ArrayList[_]]
      phonesWork.contains("33 7 12 99 11 00")
      phonesWork.size() shouldBe 1

      addressMap.size() shouldBe 2


      val m3 = mutations.get(2)
      val key3 = m3.key().asInstanceOf[Struct]
      key3.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key3.get(ConnectSchemaBuilder.TableField) shouldBe table
      var tableKeys3 = key3.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys3.schema().fields().size() shouldBe 1
      tableKeys3.get("id") shouldBe "11c11111-82a1-3a00-93d1-46196ee77204"

      cdcStruct = m3.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value3 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value3.schema().fields().map(_.name()).toVector.sortBy(identity) shouldBe Vector("id", "name", "addresses", "direct_reports", "other_reports").sortBy(identity)
      value3.get("id") shouldBe "11c11111-82a1-3a00-93d1-46196ee77204"

      value3.get("addresses") == null shouldBe true
      value3.get("name") == null shouldBe true
      value3.get("direct_reports") != null shouldBe true

      val reports = value3.get("direct_reports").asInstanceOf[util.ArrayList[Struct]].sortBy(_.get("lastname").asInstanceOf[String])
      reports.size() shouldBe 2
      val report2 = reports.get(1)
      report2.get("firstname") shouldBe "Jean-Claude"
      report2.get("lastname") shouldBe "Van Damme"

      val report1 = reports.get(0)
      report1.get("firstname") shouldBe "Arnold"
      report1.get("lastname") shouldBe "Schwarzenegger"


      val m4 = mutations.get(3)
      val key4 = m4.key().asInstanceOf[Struct]
      key4.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key4.get(ConnectSchemaBuilder.TableField) shouldBe table
      var tableKeys4 = key4.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys4.schema().fields().size() shouldBe 1
      tableKeys4.get("id") shouldBe "22c11111-82a1-3a00-93d1-46196ee77204"

      cdcStruct = m4.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.INSERT.toString

      val value4 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value4.schema().fields().map(_.name()).toVector.sortBy(identity) shouldBe Vector("id", "name", "addresses", "direct_reports", "other_reports").sortBy(identity)
      value4.get("id") shouldBe "22c11111-82a1-3a00-93d1-46196ee77204"

      value4.get("addresses") == null shouldBe true
      value4.get("name") == null shouldBe true
      value4.get("direct_reports") == null shouldBe true
      value4.get("other_reports") != null shouldBe true

      val dependencies = value4.get("other_reports").asInstanceOf[util.ArrayList[Struct]].sortBy(_.get("lastname").asInstanceOf[String])
      dependencies.size() shouldBe 2

      val d1 = reports.get(0)
      d1.get("firstname") shouldBe "Arnold"
      d1.get("lastname") shouldBe "Schwarzenegger"

      val d2 = reports.get(1)
      d2.get("firstname") shouldBe "Jean-Claude"
      d2.get("lastname") shouldBe "Van Damme"

      val m5 = mutations.get(4)
      val key5 = m5.key().asInstanceOf[Struct]
      key5.get(ConnectSchemaBuilder.KeyspaceField) shouldBe keyspace
      key5.get(ConnectSchemaBuilder.TableField) shouldBe table
      var tableKeys5 = key5.get(ConnectSchemaBuilder.KeysField).asInstanceOf[Struct]
      tableKeys5.schema().fields().size() shouldBe 1
      tableKeys5.get("id") shouldBe "62c36092-82a1-3a00-93d1-46196ee77204"

      cdcStruct = m5.value().asInstanceOf[Struct]
      metadataStruct = cdcStruct.get(ValueStructBuilder.MetadataField).asInstanceOf[Struct]
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.DELETE_COLUMN.toString

      metadataStruct.get(ConnectSchemaBuilder.DeletedColumnsField) == null shouldBe false
      metadataStruct.get(ConnectSchemaBuilder.DeletedColumnsField) shouldBe new util.ArrayList[String](Seq("name.firstname"))
      metadataStruct.get(ConnectSchemaBuilder.ChangeTypeField) shouldBe ChangeType.DELETE_COLUMN.toString

      val value5 = cdcStruct.get(ValueStructBuilder.CdcField).asInstanceOf[Struct]
      value5.schema().fields().map(_.name()).toVector.sortBy(identity) shouldBe Vector("id", "name", "addresses", "direct_reports", "other_reports").sortBy(identity)
      value5.get("id") shouldBe "62c36092-82a1-3a00-93d1-46196ee77204"
      value5.get("addresses") == null shouldBe true
      value5.get("direct_reports") == null shouldBe true
      value5.get("other_reports") == null shouldBe true
      value5.get("name") == null shouldBe true

      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0
      cdc.getMutations().size shouldBe 0

      Thread.sleep(2000)
      cdc.close()

      cdcFolder.toFile.listFiles().length shouldBe 0
      // EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra("datamountaineer")
    }

    def createCdcAndFlush(keyspace: String, table: String): CdcCassandra = {
      val cdcMgr = CommitLog.instance.segmentManager.asInstanceOf[CommitLogSegmentManagerCDC]
      cdcMgr == null shouldBe false

      implicit val config = CdcConfig(
        CassandraConfig("127.0.0.1",
          9042,
          yamlPath.toString,
          None,
          None,
          None
        ),
        Seq(
          CdcSubscription(keyspace, table, "topicOut")
        ),
        10000
      )
      val cdc = new CdcCassandra()
      cdc.start(None)
      Keyspace.open(keyspace).getColumnFamilyStore(table).forceBlockingFlush()
      CommitLog.instance.forceRecycleAllSegments()
      cdcMgr.awaitManagementCompletion()
      cdc
    }

  }
}

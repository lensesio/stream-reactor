package com.landoop.streamreactor.connect.hive.it

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Database
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util
import scala.util.Try

case class ServiceAndPort(service: String, port: Int)

trait HiveTestConfig extends Matchers {

  val ThriftService = "hive-metastore"
  val ThriftServicePort = 9083
  val NamenodeService = "namenode"
  val NamenodeServicePort = 8020

  val services = Seq(
    ServiceAndPort("datanode", 50075),
    ServiceAndPort("datanode", 50010),
    ServiceAndPort(ThriftService, ThriftServicePort),
    ServiceAndPort(NamenodeService, NamenodeServicePort)
  )

  val path = getClass().getClassLoader.getResource("docker-compose.yml").getPath
  val file = new File(path)

  val metaStoreClientUri = s"thrift://$ThriftService:$ThriftServicePort"
  val nameNodeUri = s"hdfs://$NamenodeService:$NamenodeServicePort"

  val dockerContainers = DockerComposeContainer(
    composeFiles = DockerComposeContainer.fileToEither(file),
    exposedServices = services.map(svc => ExposedService(svc.service, svc.port, 1)),
    localCompose = false,
  )

  def testInit(dbname: String, container: DockerComposeContainer = null): (HiveMetaStoreClient, FileSystem) = {
    (createClient(dbname), createFileSystem())
  }

  private def createClient(dbname: String) = {
    val hiveConf = new HiveConf()
    hiveConf.set("hive.metastore", "thrift")
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreClientUri)
    //hiveConf.set(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "false")
    hiveConf.set("dfs.datanode.use.datanode.hostname", "true")

    val client: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)
    Try {
      client.dropDatabase(dbname)
    }

    val database = new Database(dbname, null, s"/user/hive/warehouse/$dbname", new util.HashMap())
    client.createDatabase(database)
    client
  }

  private def createFileSystem() = {
    val conf: Configuration = new Configuration()
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, nameNodeUri)
    conf.set("dfs.datanode.use.datanode.hostname", "true")

    val fs: FileSystem = FileSystem.get(conf)
    fs.getUri.toString should be(nameNodeUri)
    fs
  }
}

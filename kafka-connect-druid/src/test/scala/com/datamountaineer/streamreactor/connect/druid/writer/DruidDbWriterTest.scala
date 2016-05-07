//package com.datamountaineer.streamreactor.connect.druid.writer
//
//import java.nio.file.Paths
//
//import com.datamountaineer.streamreactor.connect.config.PayloadFields
//import com.datamountaineer.streamreactor.connect.druid.config.DruidSinkSettings
//import org.apache.kafka.connect.data.Struct
//import org.apache.kafka.connect.sink.SinkRecord
//import org.kitesdk.minicluster.{HBaseService, HdfsService, MiniCluster, ZookeeperService}
//import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
//
//
//class DruidDbWriterTest extends WordSpec with Matchers with BeforeAndAfter {
//
//  before {
//    val workDir = "target/kite-minicluster-workdir-druid"
//    val miniCluster = new MiniCluster
//    .Builder()
//      .workDir(workDir)
//      .bindIP("localhost")
//      .zkPort(2181)
//      .addService(classOf[ZookeeperService])
//      .clean(true).build
//    miniCluster.start()
//    Thread.sleep(2000)
//  }
//
//  "DruidDbWriter" should {
//    "write new records to druid" in {
//
//      val configFile = Paths.get(getClass.getResource("/example.json").toURI).toAbsolutePath.toFile
//      val file = scala.io.Source.fromFile(configFile).mkString
//      val payloadFields = PayloadFields(true, Map.empty)
//      val settings = DruidSinkSettings("wikipedia", file, payloadFields)
//      val writer = DruidDbWriter(settings)
//
//      val schema = WikipediaSchemaBuilderFn()
//      val struct = new Struct(schema)
//        .put("page", "Kafka Connect")
//        .put("language", "en")
//        .put("user", "datamountaineer")
//        .put("unpatrolled", true)
//        .put("newPage", true)
//        .put("robot", false)
//        .put("anonymous", false)
//        .put("namespace", "article")
//        .put("continent", "Europe")
//        .put("country", "UK")
//        .put("region", "Greater London")
//        .put("city", "LDN")
//
//      val sinkRecord = new SinkRecord("topic", 1, null, null, schema, struct, 0)
//      writer.write(Seq(sinkRecord))
//    }
//  }
//}

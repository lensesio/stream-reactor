//package com.datamountaineer.streamreactor.connect.druid.writer
//
//import java.nio.file.{Path, Paths}
//
//import com.datamountaineer.streamreactor.connect.druid.{CuratorRequiringSuite, DruidIntegrationSuite, TestBase}
//import com.datamountaineer.streamreactor.connect.druid.config.{DruidSinkConfig, DruidSinkSettings}
//import org.apache.kafka.connect.data.Struct
//import org.apache.kafka.connect.sink.SinkRecord
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
//import scala.io.Source
//
//
//class DruidDbWriterTest extends FunSuite with TestBase with Matchers with BeforeAndAfter
//  with DruidIntegrationSuite with CuratorRequiringSuite {
//
//  test("DruidDbWriter")  {
//   // withDruidStack {
//      //(curator, broker, coordinator, overlord) => {
//       // val zkConnect = curator.getZookeeperClient.getCurrentConnectionString
//        ///modifyFile(zkConnect)
//        val config = new DruidSinkConfig(getProps())
//        val settings = DruidSinkSettings(config)
//
//        val writer =  new DruidDbWriter(settings)
//
//        val schema = WikipediaSchemaBuilderFn()
//        val struct = new Struct(schema)
//          .put("page", "Kafka Connect")
//          .put("language", "en")
//          .put("user", "datamountaineer")
//          .put("unpatrolled", true)
//          .put("newPage", true)
//          .put("robot", false)
//          .put("anonymous", false)
//          .put("namespace", "article")
//          .put("continent", "Europe")
//          .put("country", "UK")
//          .put("region", "Greater London")
//          .put("city", "LDN")
//
//        val sinkRecord = new SinkRecord(TOPIC, 1, null, null, schema, struct, 0)
//        writer.write(Seq(sinkRecord))
//      }
//
//    //}
//  //}
//}

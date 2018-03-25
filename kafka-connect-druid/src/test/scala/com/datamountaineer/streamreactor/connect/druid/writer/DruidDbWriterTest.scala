/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

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

///*
// * *
// *   * Copyright 2016 Datamountaineer.
// *   *
// *   * Licensed under the Apache License, Version 2.0 (the "License");
// *   * you may not use this file except in compliance with the License.
// *   * You may obtain a copy of the License at
// *   *
// *   * http://www.apache.org/licenses/LICENSE-2.0
// *   *
// *   * Unless required by applicable law or agreed to in writing, software
// *   * distributed under the License is distributed on an "AS IS" BASIS,
// *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *   * See the License for the specific language governing permissions and
// *   * limitations under the License.
// *   *
// */
//
//package com.datamountaineer.streamreactor.connect.coap
//
//import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConfig, CoapSettings}
//import com.datamountaineer.streamreactor.connect.coap.connection.CoapManager
//import org.scalatest.{BeforeAndAfter, WordSpec}
//
///**
//  * Created by andrew@datamountaineer.com on 30/12/2016.
//  * stream-reactor
//  */
//class TestDiscovery extends WordSpec with BeforeAndAfter with TestBase {
//
//  val server = new Server(SOURCE_PORT_SECURE, SOURCE_PORT_INSECURE)
//
//
//  before {
//    server.start()
//  }
//
//  after {
//    server.stop()
//  }
//
//  "jalds" in {
//    val props = getPropsInsecureDisco
//    val config = CoapConfig(props)
//    val setting = CoapSettings(config, true).head
//    val manager = new CoapManager(setting) {}
//  }
//
//}

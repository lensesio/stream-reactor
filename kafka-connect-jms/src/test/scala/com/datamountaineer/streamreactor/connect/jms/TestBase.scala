/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.datamountaineer.streamreactor.connect.jms


import com.datamountaineer.streamreactor.connect.jms.config.{DestinationSelector, JMSConfigConstants}
import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema, Encoder}
import com.google.protobuf.Timestamp
import com.sksamuel.avro4s.{AvroOutputStream, SchemaFor}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import com.datamountaineer.streamreactor.connect.jms.config.DestinationSelector
import com.datamountaineer.streamreactor.connect.jms.config.JMSConfigConstants
import com.sksamuel.avro4s.AvroSchema
import org.apache.activemq.jndi.ActiveMQInitialContextFactory
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.BufferedWriter
import java.io.FileWriter
import java.nio.file.Paths
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * Created by andrew@datamountaineer.com on 14/03/2017.
 * stream-reactor
 */
trait TestBase extends AnyWordSpec with Matchers with MockitoSugar {

  case class Student(name: String, age: Int, note: Double)

  val MESSAGE_SELECTOR        = "a > b"
  val JMS_USER                = ""
  val JMS_PASSWORD            = ""
  val CONNECTION_FACTORY      = "ConnectionFactory"
  val INITIAL_CONTEXT_FACTORY = classOf[ActiveMQInitialContextFactory].getCanonicalName
  val JMS_URL                 = "tcp://localhost:61620"
  val QUEUE_CONVERTER         = s"`com.datamountaineer.streamreactor.connect.converters.source.AvroConverter`"
  val QUEUE_CONVERTER_JMS = s"`com.datamountaineer.streamreactor.connect.jms.sink.converters.ProtoMessageConverter`"
  val FORMAT = "AVRO"
  val PROTO_FORMAT = "PROTOBUF"
  val SUBSCRIPTION_NAME = "subscriptionName"
  val AVRO_FILE               = getSchemaFile()

  def getAvroProp(topic: String) = s"${topic}=${AVRO_FILE}"
  def getKCQL(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source WITHTYPE $jmsType"
  def getKCQLAvroSinkConverter(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source WITHTYPE $jmsType WITHCONVERTER=$QUEUE_CONVERTER_JMS"

  def getKCQLFormat(target: String, source: String, jmsType: String, format: String) = s"INSERT INTO $target SELECT * FROM $source WITHFORMAT $format WITHTYPE $jmsType"
  def getKCQLStoreAsAddressedPerson(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source  STOREAS `datamountaineer.streamreactor.example.AddressedPerson` WITHTYPE $jmsType"
  def getKCQLEmptyStoredAsNonAddressedPerson(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source STOREAS `datamountaineer.streamreactor.example.NonAddressedPerson` WITHTYPE $jmsType"
  def getKCQLStoreAsTimedPerson(target: String, source: String, jmsType: String, path: String) = s"INSERT INTO $target SELECT * FROM $source STOREAS `datamountaineer.streamreactor.example.TimedPerson`(proto_path = $path, proto_file = `TimedPerson.proto`) WITHTYPE $jmsType WITHFORMAT $PROTO_FORMAT"
  def getKCQLStoreAsWithFileAndPath(target: String, source: String, jmsType: String, file: String, path: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS `datamountaineer.streamreactor.example.NonAddressedPerson`(proto_path = $path, proto_file = $file) WITHTYPE $jmsType"
  def getKCQLStoredAsWithNameOnly(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT * FROM $source STOREAS `com.datamountaineer.streamreactor.example.NonAddressedPerson`  WITHTYPE $jmsType"
  def getKCQLStoredAsWithInvalidData(target: String, source: String, jmsType: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS NonAddressedPersonOuterClass  WITHTYPE $jmsType"
  def getKCQLStoredAsWithInvalidPackageNameWithProtopath(target: String, source: String, jmsType: String, path: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS NonAddressedPerson(proto_path = $path)  WITHTYPE $jmsType"
  def getKCQLStoredAsWithProtopath(target: String, source: String, jmsType: String, path: String) = s"INSERT INTO $target SELECT col1,col2 FROM $source STOREAS `datamountaineer.streamreactor.example.alien.AlienPerson`(proto_path = $path)  WITHTYPE $jmsType"
  def getKCQLAvroSource(topic: String, queue: String, jmsType: String) = s"INSERT INTO $topic SELECT * FROM $queue WITHTYPE $jmsType WITHCONVERTER=$QUEUE_CONVERTER WITHSUBSCRIPTION=$SUBSCRIPTION_NAME"
  def getKCQL(target:    String, source: String, jmsType: String) =
    s"INSERT INTO $target SELECT * FROM $source WITHTYPE $jmsType"
  def getKCQLAvroSource(topic: String, queue: String, jmsType: String) =
    s"INSERT INTO $topic SELECT * FROM $queue WITHTYPE $jmsType WITHCONVERTER=$QUEUE_CONVERTER"

  def getSchemaFile(): String = {
    val schemaFile = Paths.get(UUID.randomUUID().toString)
    val schema     = AvroSchema[Student]
    val bw         = new BufferedWriter(new FileWriter(schemaFile.toFile))
    bw.write(schema.toString)
    bw.close()
    schemaFile.toAbsolutePath.toString
  }

  def getSinkProps(
    kcql:             String,
    topics:           String,
    url:              String,
    customProperties: Map[String, String] = Map(),
  ): util.Map[String, String] =
    (Map("topics" -> topics) ++ getProps(kcql, url) ++ customProperties).asJava

  def getProps(kcql: String, url: String): Map[String, String] =
    Map(
      JMSConfigConstants.KCQL                    -> kcql,
      JMSConfigConstants.JMS_USER                -> JMS_USER,
      JMSConfigConstants.JMS_PASSWORD            -> JMS_PASSWORD,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY -> INITIAL_CONTEXT_FACTORY,
      JMSConfigConstants.CONNECTION_FACTORY      -> CONNECTION_FACTORY,
      JMSConfigConstants.JMS_URL                 -> url,
      JMSConfigConstants.DESTINATION_SELECTOR    -> DestinationSelector.CDI.toString,
    )

  def kcqlWithMessageSelector(target: String, source: String, msgSelector: String) =
    s"INSERT INTO $target SELECT * FROM $source WITHTYPE TOPIC WITHJMSSELECTOR=`$msgSelector`"
}
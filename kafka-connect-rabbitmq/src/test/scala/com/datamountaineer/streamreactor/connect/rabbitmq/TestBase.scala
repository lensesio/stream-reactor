package com.datamountaineer.streamreactor.connect.rabbitmq

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Paths
import java.util
import java.util.UUID

import com.datamountaineer.streamreactor.connect.rabbitmq.config.{RabbitMQConfigConstants, RabbitMQSettings}
import com.datamountaineer.streamreactor.connect.rabbitmq.client.{RabbitMQConsumer, RabbitMQProducer}
import com.datamountaineer.streamreactor.connect.serialization.AvroSerializer
import com.github.fridujo.rabbitmq.mock.MockConnectionFactory
import com.rabbitmq.client.ConnectionFactory
import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.json4s._
import org.json4s.native.Serialization._

import scala.collection.JavaConverters._
import scala.reflect.io.Path

trait TestBase extends Suite with BeforeAndAfterAll {
    case class Measurement(id: String, number: Int, timestamp: Long, value: Double)
    val SOURCES = List("SOURCE0","SOURCE1","SOURCE2","SOURCE3")
    val ROUTING_KEYS = List("ROUTING_KEY0","ROUTING_KEY1","ROUTING_KEY2","ROUTING_KEY3")
    val TARGETS = List("TARGET0","TARGET1","TARGET2","TARGET3")
    val CONVERTERS_PACKAGE = "com.datamountaineer.streamreactor.connect.converters.source"
    val CONVERTERS = List(s"`$CONVERTERS_PACKAGE.JsonSimpleConverter`",s"`$CONVERTERS_PACKAGE.JsonConverterWithSchemaEvolution`",
        s"`$CONVERTERS_PACKAGE.AvroConverter`")
    val HOST = "192.168.10.70"
    val USERNMAME = "admin"
    val PASSWORD = "admin"
    val PORT = "20050"
    val VIRTUAL_HOST = "/rabbitmq-endpoint"
    val POLLING_TIMEOUT = "300"
    val AVRO_FILE = getSchemaFile()
    val measurement = Measurement("gSOFG8FJSD9Sd",139,1534174039,42.1)
    val PUBLISH_WAIT_TIME = 1000 //millis
    implicit val formats = DefaultFormats
    implicit val schema = SchemaFor[Measurement]()
    implicit val recordFormat = RecordFormat[Measurement]
    object TEST_MESSAGES {
        val STRING = "This is a test message".getBytes("UTF-8")
        val JSON = write(measurement).getBytes("UTF-8")
        val AVRO = AvroSerializer.getBytes(measurement)
    }

    def getPropsNoKCQL() = {
        getBasePropsNoKCQL().asJava
    }

    def getProps1KCQLBaseNoHost() = {
        (getBasePropsNoKCQL() ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> getKCQLSourceString(TARGETS(0),SOURCES(0))) -
            RabbitMQConfigConstants.HOST_CONFIG).asJava
    }

    def getProps1KCQLBase(port:String = PORT,
                                  pollingTimeout:String = POLLING_TIMEOUT) = {
        (getBasePropsNoKCQL(port,pollingTimeout) ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> getKCQLSourceString(TARGETS(0),SOURCES(0)))
            ).asJava
    }

    def getProps1KCQLNonExistingConverterClass() = {
        (getBasePropsNoKCQL() ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> getKCQLSourceString(TARGETS(0),SOURCES(0),"`com.somepackage.someconverter`"))
            ).asJava
    }

    def getProps1KCQLProvidedClassNotAConverterClass() = {
        (getBasePropsNoKCQL() ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> getKCQLSourceString(TARGETS(0),SOURCES(0),s"`${this.getClass.getCanonicalName}`"))
            ).asJava
    }

    def getProps4KCQLBase() = {
        (getBasePropsNoKCQL() ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> (0 to SOURCES.length-1).map(i => getKCQLSourceString(TARGETS(i),SOURCES(i))).mkString(";"))
            ).asJava
    }

    def getProps4KCQLsWithAllConverters() = {
        val converterClass = List("") ++ CONVERTERS
        (getBasePropsNoKCQL() ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> (0 to SOURCES.length-1).map(i => getKCQLSourceString(TARGETS(i),SOURCES(i),converterClass(i))).mkString(";"),
                RabbitMQConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_CONFIG -> s"${SOURCES(3)}=$AVRO_FILE")
            ).asJava
    }

    def getProps4KCQLsWithAllParameters() = {
        val converterClass = List("") ++ CONVERTERS
        (getBasePropsNoKCQL() ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> (0 to SOURCES.length-1).map(i => getKCQLSourceString(TARGETS(i),SOURCES(i),converterClass(i),ROUTING_KEYS(i))).mkString(";"),
                RabbitMQConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_CONFIG -> s"${SOURCES(3)}=$AVRO_FILE")
            ).asJava
    }

    def getMockedRabbitMQConsumer(props: util.Map[String,String]): RabbitMQConsumer = {
        val settings = RabbitMQSettings(props)
        new RabbitMQConsumer(settings) {
            override protected def getConnectionFactory(): ConnectionFactory = new MockConnectionFactory()
        }
    }

    def getMockedRabbitMQProducer(props: util.Map[String,String]): RabbitMQProducer = {
        val settings = RabbitMQSettings(props)
        new RabbitMQProducer(settings) {
            override protected def getConnectionFactory(): ConnectionFactory = new MockConnectionFactory()
        }
    }

    override def afterAll(): Unit = {
        Path(AVRO_FILE).delete()
    }

    def getPrivateField(o: AnyRef,clazz: Class[_],fieldName: String): Any = {
        val field = clazz.getDeclaredField(fieldName)
        field.setAccessible(true)
        field.get(o)
    }

    private def getSchemaFile(): String = {
        val schemaFile = Paths.get(s"schema_${UUID.randomUUID().toString}")
        val schema = SchemaFor[Measurement]()
        val bw = new BufferedWriter(new FileWriter(schemaFile.toFile))
        bw.write(schema.toString)
        bw.close()
        schemaFile.toAbsolutePath.toString
    }

    def getKCQLSourceString(target: String,source: String,converter: String = "",routingkey: String = ""): String = {
        val baseKCQL = s"INSERT INTO $target SELECT * FROM $source"
        val withConverter = s"WITHCONVERTER=$converter"
        val withKey = s"WITHKEY ($routingkey)"


        var kcqlString = baseKCQL

        kcqlString = converter match {
            case "" => s"$kcqlString"
            case _ => s"$kcqlString $withConverter"
        }

        kcqlString = routingkey match {
            case "" => kcqlString
            case _ => s"$kcqlString $withKey"
        }

        kcqlString
    }

    private def getBasePropsNoKCQL(port:String = PORT,
                                   pollingTimeout:String = POLLING_TIMEOUT): Map[String,String] = {
        Map(RabbitMQConfigConstants.HOST_CONFIG -> HOST,
            RabbitMQConfigConstants.USER_CONFIG -> USERNMAME,
            RabbitMQConfigConstants.PASSWORD_CONFIG -> PASSWORD,
            RabbitMQConfigConstants.PORT_CONFIG -> port,
            RabbitMQConfigConstants.VIRTUAL_HOST_CONFIG -> VIRTUAL_HOST,
            RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG -> pollingTimeout)
    }
}

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
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.json4s._
import org.json4s.native.Serialization._

import scala.collection.JavaConverters._
import scala.reflect.io.Path

trait TestBase extends Suite with BeforeAndAfterAll {
    case class Measurement(id: String, number: Int, timestamp: Long, value: Double)
    val SOURCES = List("SOURCE0","SOURCE1","SOURCE2","SOURCE3")
    val ROUTING_KEYS = List("ROUTING_KEY0","ROUTING_KEY1","ROUTING_KEY2","TOPIC.ROUTING.KEY")
    val EXCHANGE_TYPE = List("fanout","direct","topic","headers")
    val TARGETS = List("TARGET0","TARGET1","TARGET2","TARGET3")
    val CONVERTERS_PACKAGE = "com.datamountaineer.streamreactor.connect.converters.source"
    val CONVERTERS = List(s"`$CONVERTERS_PACKAGE.JsonSimpleConverter`",s"`$CONVERTERS_PACKAGE.JsonConverterWithSchemaEvolution`",
        s"`$CONVERTERS_PACKAGE.AvroConverter`")
    val HOST = "192.168.10.70"
    val USERNMAME = "admin"
    val PASSWORD = "admin"
    val PORT = "20050"
    val USE_TLS = "false"
    val VIRTUAL_HOST = "/rabbitmq-endpoint"
    val POLLING_TIMEOUT = "300"
    val AVRO_FILE = getSchemaFile()
    val measurement = Measurement("gSOFG8FJSD9Sd",139,1534174039,42.1)
    val PUBLISH_WAIT_TIME = 1000 //millis
    implicit val formats = DefaultFormats
    implicit val avroMeasurementSchema = SchemaFor[Measurement]()
    implicit val recordFormat = RecordFormat[Measurement]
    object TEST_MESSAGES {
        val STRING_BYTES = "This is a test message".getBytes("UTF-8")
        val JSON_STRING = write(measurement)
        val JSON_BYTES = JSON_STRING.getBytes("UTF-8")
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
        (getBasePropsNoKCQL(port,USE_TLS,pollingTimeout) ++
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
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> (0 to SOURCES.length-1).map(i => getKCQLSourceString(TARGETS(i),SOURCES(i),converterClass(i),ROUTING_KEYS(i),EXCHANGE_TYPE(i))).mkString(";"),
                RabbitMQConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_CONFIG -> s"${SOURCES(3)}=$AVRO_FILE")
            ).asJava
    }

    def getProps4KCQLsWithAllParametersNoConverters() = {
        val converterClass = List("") ++ CONVERTERS
        (getBasePropsNoKCQL() ++
            Map(RabbitMQConfigConstants.KCQL_CONFIG -> (0 to SOURCES.length-1).map(i => getKCQLSourceString(TARGETS(i),SOURCES(i),"",ROUTING_KEYS(i),EXCHANGE_TYPE(i))).mkString(";"))
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

    def getMeasurementSchema(): Schema = {
        SchemaBuilder.struct
            .field("id", Schema.STRING_SCHEMA)
            .field("number", Schema.INT32_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA)
            .field("value", Schema.FLOAT64_SCHEMA)
            .build()
    }

    def getMeasurementStruct(schema: Schema): Struct = {
        new Struct(schema)
            .put("id", "gSOFG8FJSD9Sd")
            .put("number", 139.toInt)
            .put("timestamp", 1534174039L)
            .put("value", 42.1)
    }

    def getKCQLSourceString(target: String,source: String,converter: String = "",withTag: String = "",withType: String = ""): String = {
        val baseKCQL = s"INSERT INTO $target SELECT * FROM $source"
        val withTypeKCQL = s"WITHTYPE $withType"
        val withConverterKCQL = s"WITHCONVERTER=$converter"
        val withKeyKCQL = s"WITHTAG ($withTag)"


        var kcqlString = baseKCQL

        kcqlString = withTag match {
            case "" => kcqlString
            case _ => s"$kcqlString $withKeyKCQL"
        }

        kcqlString = withType match {
            case "" => kcqlString
            case _ => s"$kcqlString $withTypeKCQL"
        }

        kcqlString = converter match {
            case "" => s"$kcqlString"
            case _ => s"$kcqlString $withConverterKCQL"
        }

        kcqlString
    }

    private def getBasePropsNoKCQL(port:String = PORT,
                                   useTls:String = USE_TLS,
                                   pollingTimeout:String = POLLING_TIMEOUT): Map[String,String] = {
        Map(RabbitMQConfigConstants.HOST_CONFIG -> HOST,
            RabbitMQConfigConstants.USER_CONFIG -> USERNMAME,
            RabbitMQConfigConstants.PASSWORD_CONFIG -> PASSWORD,
            RabbitMQConfigConstants.PORT_CONFIG -> port,
            RabbitMQConfigConstants.VIRTUAL_HOST_CONFIG -> VIRTUAL_HOST,
            RabbitMQConfigConstants.USE_TLS_CONFIG -> useTls,
            RabbitMQConfigConstants.POLLING_TIMEOUT_CONFIG -> pollingTimeout)
    }
}

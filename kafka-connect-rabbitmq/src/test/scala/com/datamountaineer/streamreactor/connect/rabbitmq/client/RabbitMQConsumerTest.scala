package com.datamountaineer.streamreactor.connect.rabbitmq.client

import com.datamountaineer.streamreactor.connect.rabbitmq.TestBase
import org.apache.kafka.connect.data.Struct
import org.scalatest.{Matchers, WordSpec}

class RabbitMQConsumerTest extends WordSpec with TestBase with Matchers {
    private val props = getProps4KCQLsWithAllConverters()
    private val consumer = getMockedRabbitMQConsumer(props)
    consumer.start()
    val publishChannel = consumer.connection.createChannel()

    "RabbitMQConsumer" should {
        "return None if no elements have been consumed" in {
            consumer.getRecords() shouldBe None
        }

        "be able to use the ByteConverter to generate SourceRecords" in {
            publishChannel.basicPublish("",QUEUES(0),null,TEST_MESSAGES.STRING)

            Thread.sleep(PUBLISH_WAIT_TIME)

            consumer.getRecords() match {
                case Some(x) => {
                    x.size() shouldBe 1
                    x.get(0).value() shouldBe TEST_MESSAGES.STRING
                }
                case None => fail("No messages received")
            }
        }

        "be able to use the JsonSimpleConverter to generate SourceRecords" in {
            publishChannel.basicPublish("",QUEUES(1),null,TEST_MESSAGES.JSON)

            Thread.sleep(PUBLISH_WAIT_TIME)

            consumer.getRecords() match {
                case Some(records) => {
                    records.size() shouldBe 1
                    records.get(0).value() shouldBe a [Struct]
                    val entries = records.get(0).value().asInstanceOf[Struct]
                    entries.get("id") shouldBe measurement.id
                    entries.get("number") shouldBe measurement.number
                    entries.get("timestamp") shouldBe measurement.timestamp
                    entries.get("value") shouldBe measurement.value
                }
                case None => fail("No messages received")
            }
        }

        "be able to use the JsonConverterWithSchemaEvolution to generate SourceRecords" in {
            publishChannel.basicPublish("",QUEUES(2),null,TEST_MESSAGES.JSON)
            Thread.sleep(PUBLISH_WAIT_TIME)

            consumer.getRecords() match {
                case Some(records) => {
                    records.size() shouldBe 1
                    records.get(0).value() shouldBe a [Struct]
                    val entries = records.get(0).value().asInstanceOf[Struct]
                    entries.get("id") shouldBe measurement.id
                    entries.get("number") shouldBe measurement.number
                    entries.get("timestamp") shouldBe measurement.timestamp
                    entries.get("value") shouldBe measurement.value
                }
                case None => fail("No messages received")
            }
        }

        "be able to use the AvroConverter to generate SourceRecords" in {
            publishChannel.basicPublish("",QUEUES(3),null,TEST_MESSAGES.AVRO)

            Thread.sleep(PUBLISH_WAIT_TIME)

            consumer.getRecords() match {
                case Some(records) => {
                    records.size() shouldBe 1
                    records.get(0).value() shouldBe a [Struct]
                    val entries = records.get(0).value().asInstanceOf[Struct]
                    entries.get("id") shouldBe measurement.id
                    entries.get("number") shouldBe measurement.number
                    entries.get("timestamp") shouldBe measurement.timestamp
                    entries.get("value") shouldBe measurement.value
                }
                case None => fail("No messages received")
            }
        }

        "return the elements consumed from RabbitMQ exactly once and discard them" in {
            for (i <- 1 to 260) publishChannel.basicPublish("",QUEUES(0),null,TEST_MESSAGES.STRING)
            for (i <- 1 to 210) publishChannel.basicPublish("",QUEUES(1),null,TEST_MESSAGES.JSON)
            for (i <- 1 to 290) publishChannel.basicPublish("",QUEUES(2),null,TEST_MESSAGES.JSON)
            for (i <- 1 to 240) publishChannel.basicPublish("",QUEUES(3),null,TEST_MESSAGES.AVRO)

            Thread.sleep(PUBLISH_WAIT_TIME)

            consumer.getRecords() match {
                case Some(x) => x.size() shouldBe 1000
                case None => fail("Messages were not received exactly once")
            }

            consumer.getRecords() shouldBe None
        }

        "stop consuming messages from all queues when stopped" in {
            //Clear internal message queue
            consumer.getRecords()
            consumer.stop()

            for (i <- 1 to 10) publishChannel.basicPublish("",QUEUES(0),null,TEST_MESSAGES.STRING)
            for (i <- 1 to 10) publishChannel.basicPublish("",QUEUES(1),null,TEST_MESSAGES.JSON)
            for (i <- 1 to 10) publishChannel.basicPublish("",QUEUES(2),null,TEST_MESSAGES.JSON)
            for (i <- 1 to 10) publishChannel.basicPublish("",QUEUES(3),null,TEST_MESSAGES.AVRO)

            Thread.sleep(PUBLISH_WAIT_TIME)

            consumer.getRecords() shouldBe None
        }
    }
}
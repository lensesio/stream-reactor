package com.datamountaineer.streamreactor.connect.rabbitmq.client

import com.datamountaineer.streamreactor.connect.rabbitmq.TestBase
import org.apache.kafka.connect.data.Struct
import org.scalatest.{Matchers, WordSpec}

class RabbitMQProducerTest extends WordSpec with TestBase with Matchers {
    private val props = getProps4KCQLsWithAllConverters()
    private val producer = getMockedRabbitMQConsumer(props)
    producer.start()
//    val publishChannel = .connection.createChannel()

    "RabbitMQProducer" should {
        "return None if no elements have been consumed" in {
//            consumer.getRecords() shouldBe None

        }

        "test" in {

        }

    }
}
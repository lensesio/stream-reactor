package com.datamountaineer.streamreactor.connect.rabbitmq.source

import com.datamountaineer.streamreactor.connect.rabbitmq.TestBase
import com.datamountaineer.streamreactor.connect.rabbitmq.client.RabbitMQConsumer
import org.scalatest.{Matchers, WordSpec}

class RabbitMQSourceTaskTest extends WordSpec with TestBase with Matchers {
    private val task = new RabbitMQSourceTask() {
        override protected  def initializeConsumer(props: java.util.Map[String,String]): RabbitMQConsumer = {
            getMockedRabbitMQConsumer(props)
        }
    }
    private val props = getProps4KCQLsWithAllConverters()
    task.start(props)
    private val consumer:RabbitMQConsumer = getPrivateField(task,classOf[RabbitMQSourceTask],"consumer").asInstanceOf[RabbitMQConsumer]
    private val publishChannel = consumer.connection.createChannel()

    "RabbitMQSourceTask" should {
        "return the consumed messages and empty the internal queue" in {
            sendMessages()

            task.poll() match {
                case x => {
                    x.size() shouldBe 40
                }
                case null => fail("No messages polled")
            }

            task.poll() shouldBe null
        }

        "stop consuming messages when the consumer has been stopped" in {
            task.stop()
            sendMessages()

            task.poll() shouldBe null
        }
    }

    private def sendMessages(): Unit = {
        for (i <- 1 to 10) publishChannel.basicPublish("",SOURCES(0),null,TEST_MESSAGES.STRING)
        for (i <- 1 to 10) publishChannel.basicPublish("",SOURCES(1),null,TEST_MESSAGES.JSON)
        for (i <- 1 to 10) publishChannel.basicPublish("",SOURCES(2),null,TEST_MESSAGES.JSON)
        for (i <- 1 to 10) publishChannel.basicPublish("",SOURCES(3),null,TEST_MESSAGES.AVRO)

        Thread.sleep(PUBLISH_WAIT_TIME)
    }
}

package com.datamountaineer.streamreactor.connect.rabbitmq.sink

import com.datamountaineer.streamreactor.connect.rabbitmq.TestBase
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class RabbitMQSinkConnectorTest extends WordSpec with TestBase with Matchers {
    "RabbitMQSourceConnector" should {
        "map props with some KCQLs to 1 maxTask when maxTasks=1" in {
            val props = getProps4KCQLsWithAllParametersNoConverters()
            val connector = getSinkConnector(props)
            val taskConfigs = connector.taskConfigs(1).asScala
            taskConfigs.size shouldBe 1
        }

        "map props with some KCQLs to an equal number of maxTasks when maxTasks>1" in {
            val props = getProps4KCQLsWithAllParametersNoConverters()
            val connector = getSinkConnector(props)
            val taskConfigs = connector.taskConfigs(5).asScala
            taskConfigs.size shouldBe 5
        }
    }

    private def getSinkConnector(props: java.util.Map[String,String]): RabbitMQSinkConnector = {
        val connector = new RabbitMQSinkConnector()
        connector.start(props)

        connector
    }
}

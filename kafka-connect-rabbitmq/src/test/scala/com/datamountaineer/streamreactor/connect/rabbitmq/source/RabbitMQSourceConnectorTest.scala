package com.datamountaineer.streamreactor.connect.rabbitmq.source

import com.datamountaineer.streamreactor.connect.rabbitmq.TestBase
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class RabbitMQSourceConnectorTest extends WordSpec with TestBase with Matchers {
    "RabbitMQSourceConnector" should {
        "map props with 1 KCQL to 1 group" in {
            val props = getProps1KCQLBase()
            val connector = getSourceConnector(props)
            val tasks = connector.taskConfigs(1).asScala
            tasks.size shouldBe 1
        }

        "map props with 1 KCQL to 1 group when maxTasks>1" in {
            val props = getProps1KCQLBase()
            val connector = getSourceConnector(props)
            val tasks = connector.taskConfigs(3).asScala
            tasks.size shouldBe 1
        }

        "map props with some KCQLs to 1 group when KCQL>maxTasks=1" in {
            val props = getProps4KCQLsWithAllConverters()
            val connector = getSourceConnector(props)
            val tasks = connector.taskConfigs(1).asScala
            tasks.size shouldBe 1
        }

        "split props with some KCQLs to an equal number of groups when KCQL=maxTasks>1" in {
            val props = getProps4KCQLsWithAllConverters()
            val connector = getSourceConnector(props)
            val tasks = connector.taskConfigs(4).asScala
            tasks.size shouldBe 4
        }

        "map props with some KCQLs to the correct ammount of groups when KCQL>maxTasks>1" in {
            val props = getProps4KCQLsWithAllConverters()
            val connector = getSourceConnector(props)
            val tasksCase1 = connector.taskConfigs(3).asScala
            tasksCase1.size shouldBe 3

            val tasksCase2 = connector.taskConfigs(2).asScala
            tasksCase2.size shouldBe 2
        }
    }

    private def getSourceConnector(props: java.util.Map[String,String]): RabbitMQSourceConnector = {
        val connector = new RabbitMQSourceConnector()
        connector.start(props)

        connector
    }
}

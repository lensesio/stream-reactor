package io.lenses.streamreactor.connect.testcontainers

import com.typesafe.scalalogging.LazyLogging

object KafkaVersions extends LazyLogging{

  private val FallbackConfluentVersion = "7.3.1"

  val ConfluentVersion: String = {
    val (vers, from) = sys.env.get("CONFLUENT_VERSION") match {
      case Some(value) => (value, "env")
      case None => (FallbackConfluentVersion, "default")
    }
    logger.info("Selected confluent version {} from {}", vers, from)
    vers
  }

}

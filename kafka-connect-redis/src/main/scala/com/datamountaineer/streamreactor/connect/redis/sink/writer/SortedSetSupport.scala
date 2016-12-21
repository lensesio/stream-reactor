package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.typesafe.scalalogging.slf4j.StrictLogging
import com.datamountaineer.connector.config.Config
import scala.collection.JavaConverters._

trait SortedSetSupport extends StrictLogging {

  // How to 'score' each message
  def getScoreField(kcqlConfig: Config): String = {
    val sortedSetParams = kcqlConfig.getStoredAsParameters.asScala
    val scoreField = if (sortedSetParams.keys.exists(k => k.equalsIgnoreCase("score")))
      sortedSetParams.find { case (k, v) => k.equalsIgnoreCase("score") }.get._2
    else {
      logger.info("You have not defined how to 'score' each message. We'll try to fall back to 'timestamp' field")
      "timestamp"
    }
    scoreField
  }

  // assert(SS.isValid, "The SortedSet definition at Redis accepts only case sensitive alphabetic characters")

}

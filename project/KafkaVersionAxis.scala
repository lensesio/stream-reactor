import KafkaVersionAxis.kafkaToConfluentVersion

case class KafkaVersionAxis(kafkaVersion: String) {

  val confluentPlatformVersion: String =
    kafkaToConfluentVersion.getOrElse(kafkaVersion, throw new IllegalStateException("unexpected kafka version"))

}

object KafkaVersionAxis {
  private val kafkaToConfluentVersion = Map(
    "2.8.1" -> "6.2.2",
    "3.3.0" -> "7.3.1",
  )

  // KafkaVersion could later be a parameter
  val kafkaVersion:     String           = "3.3.0"
  val kafkaVersionAxis: KafkaVersionAxis = KafkaVersionAxis(kafkaVersion)

}

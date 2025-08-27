package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

trait GuardConfigKeys extends WithConnectorPrefix {
  val GUARD_ENABLE                              = s"$connectorPrefix.guard.enable"
  val GUARD_BOOTSTRAP_SERVERS                   = s"$connectorPrefix.guard.bootstrap.servers"
  val GUARD_HEARTBEAT_TOPIC                     = s"$connectorPrefix.guard.heartbeat.topic"
  val GUARD_HEARTBEAT_MS                        = s"$connectorPrefix.guard.heartbeat.ms"
  val GUARD_TOPIC_AUTOCREATE_ENABLE             = s"$connectorPrefix.guard.topic.autocreate.enable"
  val GUARD_TOPIC_AUTOCREATE_PARTITIONS         = s"$connectorPrefix.guard.topic.autocreate.partitions"
  val GUARD_TOPIC_AUTOCREATE_REPLICATION_FACTOR = s"$connectorPrefix.guard.topic.autocreate.replication.factor"

  private val GUARD_ENABLE_DOC = "Enable Kafka-based guard to fence duplicate active tasks"
  private val GUARD_BOOTSTRAP_DOC = "Bootstrap servers for guard. Other client settings may be set via guard.client.*"
  private val GUARD_HEARTBEAT_TOPIC_DOC = "Topic used to write guard heartbeats (use compacted topic)"
  private val GUARD_HEARTBEAT_MS_DOC = "Heartbeat interval in milliseconds"
  private val GUARD_AUTOCREATE_ENABLE_DOC = "If true, auto-create the guard heartbeat topic when missing (cleanup.policy=compact)."
  private val GUARD_AUTOCREATE_PARTITIONS_DOC = "Partitions for auto-created heartbeat topic. 1 is sufficient for fencing."
  private val GUARD_AUTOCREATE_RF_DOC = "Replication factor for auto-created heartbeat topic. Default 1 for portability; recommend 3 in production."


  final val DEFAULTS: Map[String, Any] = Map(
    GUARD_ENABLE -> false,
    GUARD_BOOTSTRAP_SERVERS -> null,
    GUARD_HEARTBEAT_TOPIC -> s"${connectorPrefix.replace('.', '-')}-sink-guard-heartbeat",
    GUARD_HEARTBEAT_MS -> 10000L,
    GUARD_TOPIC_AUTOCREATE_ENABLE -> true,
    GUARD_TOPIC_AUTOCREATE_PARTITIONS -> 1,
    GUARD_TOPIC_AUTOCREATE_REPLICATION_FACTOR -> 1.toShort,
  )

  def addGuardSettingsToConfigDef(configDef: ConfigDef): ConfigDef =
    configDef
      .define(GUARD_ENABLE, Type.BOOLEAN, DEFAULTS(GUARD_ENABLE), Importance.HIGH, GUARD_ENABLE_DOC)
      .define(GUARD_BOOTSTRAP_SERVERS, Type.STRING, DEFAULTS(GUARD_BOOTSTRAP_SERVERS), Importance.HIGH, GUARD_BOOTSTRAP_DOC)
      .define(GUARD_HEARTBEAT_TOPIC, Type.STRING, DEFAULTS(GUARD_HEARTBEAT_TOPIC), Importance.LOW, GUARD_HEARTBEAT_TOPIC_DOC)
      .define(GUARD_HEARTBEAT_MS, Type.LONG, DEFAULTS(GUARD_HEARTBEAT_MS), Importance.LOW, GUARD_HEARTBEAT_MS_DOC)
      .define(GUARD_TOPIC_AUTOCREATE_ENABLE, Type.BOOLEAN, DEFAULTS(GUARD_TOPIC_AUTOCREATE_ENABLE), Importance.LOW, GUARD_AUTOCREATE_ENABLE_DOC)
      .define(GUARD_TOPIC_AUTOCREATE_PARTITIONS, Type.INT, DEFAULTS(GUARD_TOPIC_AUTOCREATE_PARTITIONS), Importance.LOW, GUARD_AUTOCREATE_PARTITIONS_DOC)
      .define(GUARD_TOPIC_AUTOCREATE_REPLICATION_FACTOR, Type.SHORT, DEFAULTS(GUARD_TOPIC_AUTOCREATE_REPLICATION_FACTOR), Importance.LOW, GUARD_AUTOCREATE_RF_DOC)

}

trait GuardSettings extends io.lenses.streamreactor.common.config.base.traits.BaseSettings with GuardConfigKeys {
  def getGuardEnable: Boolean = getBoolean(GUARD_ENABLE)

  def getGuardBootstrapServersOpt: Option[String] = Option(getString(GUARD_BOOTSTRAP_SERVERS)).filter(_.trim.nonEmpty)

  def getGuardHeartbeatTopic: String = getString(GUARD_HEARTBEAT_TOPIC)

  def getGuardHeartbeatMs: Long = getLong(GUARD_HEARTBEAT_MS)
}

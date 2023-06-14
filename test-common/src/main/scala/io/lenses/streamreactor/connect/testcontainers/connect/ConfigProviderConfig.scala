package io.lenses.streamreactor.connect.testcontainers.connect

case class ConfigProviders(configProviders: Seq[ConfigProvider]) {
  def toEnvMap: Map[String, String] = {
    val providersEnv = "CONNECT_CONFIG_PROVIDERS" -> configProviders.map(_.name).mkString(",")
    (providersEnv +: configProviders.flatMap(_.toEnvMap)).toMap
  }
}

case class ConfigProvider(
  name:    String,
  `class`: String,
  props:   Map[String, String],
) {
  private val classProp   = s"CONNECT_CONFIG_PROVIDERS_${name.toUpperCase}_CLASS"
  private val paramPrefix = s"CONNECT_CONFIG_PROVIDERS.${name.toUpperCase}_PARAM_"

  def toEnvMap: Map[String, String] = {
    val classEnv = classProp -> `class`
    val envs = props.map {
      case (pk, pv) => paramPrefix + pk.replace(".", "_").toUpperCase -> pv
    }
    envs + classEnv
  }
}

package io.lenses.streamreactor.common.config.base.intf;

import io.lenses.streamreactor.common.config.base.constants.TraitConfigConst;
import org.apache.kafka.common.config.ConfigException;

//TODO add implementation
public interface KcqlSettings extends BaseSettings {

  default String getKcqlConstant() {
    return connectorPrefix() + "." + TraitConfigConst.KCQL_PROP_SUFFIX;
  }

  default String[] getKcqlRaw() {
    String rawKcql = getString(getKcqlConstant());
    if (rawKcql.isEmpty()) {
      throw new ConfigException("Missing " + getKcqlConstant());
    }
    return rawKcql.split(";");
  }
}

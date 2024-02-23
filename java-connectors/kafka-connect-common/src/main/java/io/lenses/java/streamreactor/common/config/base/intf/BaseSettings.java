package io.lenses.java.streamreactor.common.config.base.intf;

import java.util.List;
import org.apache.kafka.common.config.types.Password;

public interface BaseSettings extends WithConnectorPrefix {
  String getString(String key);
  Integer getInt(String key);
  Boolean getBoolean(String key);
  Password getPassword(String key);
  List<String> getList(String key);


}

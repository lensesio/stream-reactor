package io.lenses.streamreactor.common.config.base.intf;

import io.lenses.streamreactor.common.errors.ErrorPolicy;
import io.lenses.streamreactor.common.errors.ErrorPolicyEnum;
import io.lenses.streamreactor.common.config.base.constants.TraitConfigConst;

public interface ErrorPolicySettings extends BaseSettings {
  default ErrorPolicy getErrorPolicy() {
    return ErrorPolicyEnum.byName(getString(connectorPrefix() + "." + TraitConfigConst.MAX_RETRIES_PROP_SUFFIX));
  }
}

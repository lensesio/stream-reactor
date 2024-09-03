package io.lenses.streamreactor.common.security;

import cyclops.control.Option;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
class StoreInfo {

  private String storePath;

  private Option<String> storeType;

  private Option<String> storePassword;

}

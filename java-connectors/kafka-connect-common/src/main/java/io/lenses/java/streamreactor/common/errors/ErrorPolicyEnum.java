package io.lenses.java.streamreactor.common.errors;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

public enum ErrorPolicyEnum {
  NOOP(new NoopErrorPolicy()),
  THROW(new ThrowErrorPolicy()),
  RETRY(new RetryErrorPolicy());

  private static final Map<String, ErrorPolicy> errorPolicyByName =
    Arrays.stream(ErrorPolicyEnum.values()).collect(toMap(Enum::name, ErrorPolicyEnum::getErrorPolicy));

  @Getter
  private final ErrorPolicy errorPolicy;

  ErrorPolicyEnum(ErrorPolicy errorPolicy) {
    this.errorPolicy = errorPolicy;
  }

  public static ErrorPolicy byName(String name){
    Optional<ErrorPolicy> policyByName = ofNullable(errorPolicyByName.get(name));
    return policyByName.orElseThrow(() -> new RuntimeException("Couldn't find ErrorPolicy by name: " + name));
  }
}

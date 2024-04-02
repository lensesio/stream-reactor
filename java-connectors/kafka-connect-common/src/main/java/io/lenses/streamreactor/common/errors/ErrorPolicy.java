package io.lenses.streamreactor.common.errors;

public interface ErrorPolicy {
  void handle(Throwable throwable, Boolean sink, Integer retryCount); //sink = true int = 0

  default void handle(Throwable throwable) {
    handle(throwable, true, 0);
  }
}

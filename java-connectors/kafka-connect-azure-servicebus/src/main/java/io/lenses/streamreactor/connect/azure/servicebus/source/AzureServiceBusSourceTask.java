package io.lenses.streamreactor.connect.azure.servicebus.source;

import static java.util.Optional.ofNullable;

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSourceConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

public class AzureServiceBusSourceTask extends SourceTask {

  private final JarManifest jarManifest;

  public AzureServiceBusSourceTask() {
    this.jarManifest = new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());;
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    OffsetStorageReader offsetStorageReader = ofNullable(this.context).flatMap(
        context -> ofNullable(context.offsetStorageReader())).orElseThrow();
    new AzureServiceBusSourceConfig(props);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    return null;
  }

  @Override
  public void stop() {

  }
}

package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader;
import static java.util.Optional.ofNullable;

import io.lenses.java.streamreactor.common.util.JarManifest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

@Slf4j
public class AzureEventHubsSourceTask extends SourceTask {

  private final JarManifest jarManifest;

  public AzureEventHubsSourceTask() {
    jarManifest = new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }


  @Override
  public void start(Map<String, String> props) {
    log.info("Task initialising");
    printAsciiHeader(jarManifest, "/azure-eventhubs-ascii.txt");

    Map<String, String> configProps;
    boolean contextConfigEmpty = ofNullable(context).flatMap(
        context -> ofNullable(context.configs()))
      .flatMap(configs -> Optional.of(configs.isEmpty())).orElse(false);
    if (contextConfigEmpty) {
      configProps = props;
    } else {
      assert context != null;
      configProps = context.configs();
    }
  }


  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    return null;
  }

  @Override
  public void stop() {

  }
}

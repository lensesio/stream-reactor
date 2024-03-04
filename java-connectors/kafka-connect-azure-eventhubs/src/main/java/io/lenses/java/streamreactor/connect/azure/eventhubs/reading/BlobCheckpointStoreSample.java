//package io.lenses.java.streamreactor.connect.azure.eventhubs.reading;
//
//import com.azure.core.http.policy.HttpLogDetailLevel;
//import com.azure.core.http.policy.HttpLogOptions;
//import com.azure.messaging.eventhubs.EventProcessorClient;
//import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
//import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
//import com.azure.messaging.eventhubs.models.PartitionContext;
//import com.azure.messaging.eventhubs.models.PartitionOwnership;
//import com.azure.storage.blob.BlobContainerAsyncClient;
//import com.azure.storage.blob.BlobContainerClientBuilder;
//import java.util.StringJoiner;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//public class BlobCheckpointStoreSample {
//
//  public static void main(String[] args) throws Exception {
//    String consumerGroup = "con-grp-1";
//    String eventHubInstanceName = "demo-json-1p";
//    String connectionString = "";
//
//    String blobContainerName = "checkpoints";
//    String blobConnectionString = "";
//
//    BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
//        .connectionString(blobConnectionString)
//        .containerName(blobContainerName)
//        .httpLogOptions(new HttpLogOptions().setLogLevel(HttpLogDetailLevel.BODY_AND_HEADERS))
//        .buildAsyncClient();
//
//    BlobCheckpointStore blobCheckpointStore = new BlobCheckpointStore(blobContainerAsyncClient);
//
//    EventProcessorClient eventProcessorClient = new EventProcessorClientBuilder()
//        .connectionString(connectionString)
//        .consumerGroup(consumerGroup)
//        .checkpointStore(blobCheckpointStore)
//        .eventHubName(eventHubInstanceName)
//        .processEvent(processEvent -> {
//          String bodyAsString = processEvent.getEventData().getBodyAsString();
//          log.info("Received event: " + bodyAsString);
//          System.out.println("Received event: " + bodyAsString);
//          //        processEvent.updateCheckpointAsync();
//        })
//        .processError(processError -> {
//          PartitionContext partitionContext = processError.getPartitionContext();
//          log.warn("Error happened when trying to read partition={}",
//              partitionContext.getPartitionId());
//          log.warn(partitionContext.toString());
//        })
//        .buildEventProcessorClient();
//
//    eventProcessorClient.start();
//
//    Thread.sleep(10 * 1000L);
//
//    System.out.println("Updating checkpoint");
//    //    Checkpoint checkpoint = new Checkpoint()
//    //      .setConsumerGroup(consumerGroup)
//    //      .setEventHubName(eventHubName)
//    //      .setPartitionId("0")
//    //      .setSequenceNumber(2L)
//    //      .setOffset(250L);
//    //    blobCheckpointStore.updateCheckpoint(checkpoint)
//    //      .subscribe(etag -> System.out.println(etag), error -> System.out
//    //        .println(error.getMessage()));
//
//    //    List<PartitionOwnership> pos = new ArrayList<>();
//    //    for (int i = 0; i < 5; i++) {
//    //      PartitionOwnership po = new PartitionOwnership()
//    //        .setEventHubName(eventHubName)
//    //        .setConsumerGroup(consumerGroup)
//    //        .setOwnerId("owner1")
//    //        .setPartitionId(String.valueOf(i));
//    //      pos.add(po);
//    //    }
//    //    blobCheckpointStore.claimOwnership(pos).subscribe(BlobCheckpointStoreSample::printPartitionOwnership,
//    //      System.out::println);
//  }
//
//  static void printPartitionOwnership(PartitionOwnership partitionOwnership) {
//    String po =
//        new StringJoiner(",")
//            .add("pid=" + partitionOwnership.getPartitionId())
//            .add("ownerId=" + partitionOwnership.getOwnerId())
//            .add("cg=" + partitionOwnership.getConsumerGroup())
//            .add("eh=" + partitionOwnership.getEventHubName())
//            .add("etag=" + partitionOwnership.getETag())
//            .add("lastModified=" + partitionOwnership.getLastModifiedTime())
//            .toString();
//    System.out.println(po);
//  }
//
//}

package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationStream {
    /**
     * State of the stream : New, Committed, finalised, in_progress
     */
    StreamState currentState = null;
    private static final Logger logger = LoggerFactory.getLogger(ApplicationStream.class);
    private final String tableName;
    private WriteStream stream = null;
    private JsonStreamWriter jsonWriter = null;
    private Map<TopicPartition, OffsetAndMetadata> offsetInformation;
    private final BigQueryWriteClient client;
    /**
     * Number of times append is called
     */
    private AtomicInteger appendCallsCount;
    /**
     * Number of append requests completed successfully. This can never be greater than appendCallsCount
     */
    private AtomicInteger completedCallsCount;

    /**
     * This is called by builder to guarantee sequence.
     */
    private AtomicInteger maxCallsCount;

    public ApplicationStream(String tableName, BigQueryWriteClient client) throws Exception {
        this.client = client;
        this.tableName = tableName;
        this.offsetInformation = new HashMap<>();
        this.appendCallsCount = new AtomicInteger();
        this.maxCallsCount = new AtomicInteger();
        this.completedCallsCount = new AtomicInteger();
        generateStream();
    }

    public Map<TopicPartition, OffsetAndMetadata> getOffsetInformation() {
        return offsetInformation;
    }

    private void generateStream() throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        this.stream = client.createWriteStream(tableName, WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build());
        this.jsonWriter = JsonStreamWriter.newBuilder(stream.getName(), client).build();
        currentState = StreamState.CREATED;
    }

    public void closeStream() {
        this.jsonWriter.close();
        logger.info("JSON Writer for stream {} closed", getStreamName());
    }

    public String getStreamName() {
        return this.stream.getName();
    }

    /**
     * Increases the Append call count and returns the updated value
     */
    public void increaseAppendCallCount() {
        this.appendCallsCount.incrementAndGet();
    }

    /**
     * Increases the Max call count by 1. The tells the total expected calls which would be made to append method.
     * Returns the updated value
     */
    public int increaseMaxCallsCount() {
        int count = this.maxCallsCount.incrementAndGet();
        if (currentState == StreamState.CREATED) {
            currentState = StreamState.APPEND;
        }
        return count;
    }

    /**
     * Increases the count of Append calls which are completed.
     * Returns the updated value
     */
    public void increaseCompletedCallsCount() {
        this.completedCallsCount.incrementAndGet();
    }

    /**
     * Stream can be closed for writing (not appending new data) only if its current state is different from created
     * A stream with CREATED state tells the stream has not been used for writing anything and would result in resource
     * wastage we create new without using the existing one
     *
     * @return
     */
    public boolean canBeMovedToInactive() {
        return currentState != StreamState.CREATED;
    }

    /**
     * Updates offset handled by this particular stream
     *
     * @param newOffsets - New offsets to be added on top of existing
     */
    public void updateOffsetInformation(Map<TopicPartition, OffsetAndMetadata> newOffsets) {
        offsetInformation.putAll(newOffsets);
        increaseMaxCallsCount();
    }

    public JsonStreamWriter writer() {
        return this.jsonWriter;
    }

    /**
     * @return Returns true if all append calls are completed and the completed calls is equal to maximum calls with
     * this stream
     */
    public boolean areAllExpectedCallsCompleted() {
        return (this.maxCallsCount.intValue() == this.appendCallsCount.intValue())
                && (this.appendCallsCount.intValue() == this.completedCallsCount.intValue());
    }

    public void finalise() {
        if (currentState == StreamState.APPEND) {
            FinalizeWriteStreamResponse finalizeResponse =
                    client.finalizeWriteStream(this.getStreamName());
            logger.info("Rows written: " + finalizeResponse.getRowCount());
            currentState = StreamState.FINALISED;
        } else {
            throw new BigQueryStorageWriteApiConnectException(
                    "Stream could not be finalised as current state " + currentState + " is not expected state.");
        }
    }

    public void commit() {
        if (currentState == StreamState.FINALISED) {
            BatchCommitWriteStreamsRequest commitRequest =
                    BatchCommitWriteStreamsRequest.newBuilder()
                            .setParent(tableName)
                            .addWriteStreams(getStreamName())
                            .build();
            BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(commitRequest);
            // If the response does not have a commit time, it means the commit operation failed.
            if (!commitResponse.hasCommitTime()) {
                for (StorageError err : commitResponse.getStreamErrorsList()) {
                    logger.error(err.getErrorMessage());
                }
                //TODO:Exception Handling
                throw new RuntimeException("Error committing the streams");
            }
            logger.info("Appended and committed records successfully for stream {}", getStreamName());
            currentState = StreamState.COMMITTED;
        } else {
            throw new BigQueryStorageWriteApiConnectException(
                    "Stream could not be committed as current state " + currentState + " is not expected state.");
        }
    }

    public boolean isReadyForOffsetCommit() {
        return currentState == StreamState.COMMITTED;
    }

    public void markInactive() {
        currentState = StreamState.INACTIVE;
        this.jsonWriter.close();
    }

    @Override
    public String toString() {
        return "ApplicationStream{" +
                "currentState=" + currentState +
                ", tableName='" + tableName + '\'' +
                ", offsetInformation=" + offsetInformation +
                ", appendCallsCount=" + appendCallsCount +
                ", completedCallsCount=" + completedCallsCount +
                ", maxCallsCount=" + maxCallsCount +
                '}';
    }

    public boolean isInactive() {
        return currentState == StreamState.INACTIVE;
    }
}

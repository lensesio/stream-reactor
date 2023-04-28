package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * An extension of {@link StorageWriteApiApplicationStream} which uses application streams for batch loading data
 * following at least once semantic
 * Current/Active stream means - Stream which is not yet finalised and would be used for any new data append
 * Other streams (non-current/ non-active streams) - These streams may/may not be finalised yet but would not be used
 * for any new data. These will only write data for offsets assigned so far.
 */
public class StorageWriteApiBatchApplicationStream extends StorageWriteApiApplicationStream {

    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiBatchApplicationStream.class);

    /**
     * Map of <tableName , <StreamName, {@link ApplicationStream}>>
     * Streams should be accessed in the order of entry, so we need LinkedHashMap here
     */
    protected ConcurrentMap<String, LinkedHashMap<String, ApplicationStream>> streams;

    /**
     * Quick lookup for current open stream by tableName
     */
    protected ConcurrentMap<String, String> currentStreams;

    /**
     * Lock on table names to prevent execution of critical section by multiple threads
     */
    protected ConcurrentMap<String, Object> tableLocks;

    public StorageWriteApiBatchApplicationStream(
            int retry,
            long retryWait,
            BigQueryWriteSettings writeSettings,
            boolean autoCreateTables,
            ErrantRecordHandler errantRecordHandler,
            SchemaManager schemaManager) {
        super(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler, schemaManager);
        streams = new ConcurrentHashMap<>();
        currentStreams = new ConcurrentHashMap<>();
        tableLocks = new ConcurrentHashMap<>();
    }

    /**
     * Takes care of resource cleanup
     */
    @Override
    public void preShutdown() {
        logger.debug("Shutting down all streams on all tables as due to task shutdown!!!");
        this.streams.values()
                .stream().flatMap(item -> item.values().stream())
                .collect(Collectors.toList())
                .forEach(ApplicationStream::closeStream);
        logger.debug("Shutting completed for all streams on all tables!");
    }

    /**
     * Calls storage Api's append
     *
     * @param tableName  The table to write data to in project/dataset/tableName format
     * @param rows       The records to write
     * @param streamName The stream to use to write table to table.
     */
    @Override
    public void appendRows(TableName tableName, List<Object[]> rows, String streamName) {
        JSONArray jsonRecords = new JSONArray();
        rows.forEach(item -> jsonRecords.put(item[1]));
        ApplicationStream applicationStream = this.streams.get(tableName.toString()).get(streamName);
        try {
            ApiFuture<AppendRowsResponse> response = applicationStream.writer().append(jsonRecords);
            applicationStream.increaseAppendCall();
            AppendRowsResponse writeResult = response.get();

            if (writeResult.hasAppendResult()) {
                logger.trace("Append call completed successfully on stream {}", applicationStream.getStreamName());
                applicationStream.increaseCompletedCalls();
                commitStreamIfEligible(tableName.toString(), applicationStream.getStreamName());
            } else if (writeResult.hasUpdatedSchema()) {
                //TODO: Schema Update attempt once
            } else if (writeResult.hasError()) {
                //TODO: exception handling
            }
        } catch (Exception e) {
            //TODO: Exception handling, Table creation, Schema updates, DLQ routing, Request size reduction
            throw new BigQueryConnectException(e);
        }
    }

    /**
     * Gets commitable offsets on all tables and all streams. Offsets returned should be sequential. As soon as we see a
     * stream not committed we will drop iterating over next streams for that table. Cleans up committed streams
     *
     * @return Returns Map of TopicPartition to OffsetMetadata. Will be empty if there is nothing new to commit.
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetsReadyForCommits = new ConcurrentHashMap<>();
        this.streams.forEach((tableName, streamDetails) -> {
                    synchronized (lock(tableName)) {
                        int i = 0;
                        Set<String> deletableStreams = new HashSet<>();
                        for (Map.Entry<String, ApplicationStream> applicationStreamEntry : streamDetails.entrySet()) {
                            ApplicationStream applicationStream = applicationStreamEntry.getValue();
                            String streamName = applicationStreamEntry.getKey();
                            if (applicationStream.isInactive()) {
                                logger.trace("Ignoring inactive stream {} at index {}...", streamName, i);
                            } else if (applicationStream.isReadyForOffsetCommit()) {
                                logger.trace("Pulling offsets from committed stream {} at index {} ...", streamName, i);
                                offsetsReadyForCommits.putAll(applicationStream.getOffsetInformation());
                                applicationStream.markInactive();
                            } else {
                                logger.trace("Ignoring all streams as stream {} at index {} is not yet committed", streamName, i);
                                // We move sequentially for offset commit, until current offsets are ready, we cannot commit next.
                                break;
                            }
                            deletableStreams.add(streamName);
                            i++;
                        }
                        deletableStreams.forEach(streamDetails::remove);
                    }
                }
        );

        logger.trace("Commitable offsets are {} for all tables on all eligible stream  : ", offsetsReadyForCommits);

        return offsetsReadyForCommits;
    }


    /**
     * This attempts to create stream if there are no existing stream for table or the stream is not empty
     * (it has been assigned some records)
     *
     * @param tableName Name of the table in project/dataset/table format
     * @return
     */
    @Override
    public boolean mayBeCreateStream(String tableName, List<Object[]> rows) {
        String streamName = this.currentStreams.get(tableName);
        boolean shouldCreateNewStream = (streamName == null) ||
                (this.streams.get(tableName).get(streamName) != null
                        && this.streams.get(tableName).get(streamName).canTransitionToNonActive());
        if (shouldCreateNewStream) {
            logger.trace("Attempting to create new stream on table {}", tableName);
            return this.createStream(tableName, streamName, rows);
        }
        return false;
    }

    /**
     * Assigns offsets to current stream on table
     *
     * @param tableName The name of table
     * @param rows      Offsets which are to be written by current stream to bigquery table
     * @return Stream name using which offsets would be written
     */
    @Override
    public String updateOffsetsOnStream(
            String tableName,
            List<Object[]> rows
    ) {
        String streamName;
        Map<TopicPartition, OffsetAndMetadata> offsetInfo = getOffsetFromRecords(rows);
        synchronized (lock(tableName)) {
            streamName = this.getCurrentStreamForTable(tableName, rows);
            this.streams.get(tableName).get(streamName).updateOffsetInformation(offsetInfo);
        }
        logger.trace("Assigned offsets {} to stream {}", offsetInfo, streamName);
        return streamName;
    }

    /**
     * Takes care of creating a new application stream
     *
     * @param tableName
     * @return
     */
    @VisibleForTesting
    ApplicationStream createApplicationStream(String tableName, List<Object[]> rows) {
        try {
            return new ApplicationStream(tableName, getWriteClient());
        } catch (Exception e) {
            // TODO: Table creation
            //TODO: Exception handling
            throw new BigQueryStorageWriteApiConnectException(e.getMessage());
        }

    }

    /**
     * Creates stream and updates current active stream with the newly created one
     *
     * @param tableName The name of the table
     * @param oldStream Last active stream on the table when this method was invoked.
     * @return Returns false if the oldstream is not equal to active stream , creates stream otherwise and returns true
     */
    private boolean createStream(String tableName, String oldStream, List<Object[]> rows) {
        synchronized (lock(tableName)) {
            // This check verifies if the current active stream is same as seen by the calling method. If different, that
            // would mean a new stream got created by some other thread and this attempt can be dropped.
            if (!Objects.equals(oldStream, this.currentStreams.get(tableName))) {
                return false;
            }
            // Current state is same as calling state. Create new Stream
            ApplicationStream stream = createApplicationStream(tableName, rows);
            if (stream == null) {
                if (rows == null) {
                    return false;
                } else {
                    // We should never reach here
                    throw new BigQueryStorageWriteApiConnectException(
                            "Application Stream creation could not be completed successfully.");
                }

            }
            String streamName = stream.getStreamName();

            this.streams.computeIfAbsent(tableName, t -> new LinkedHashMap<>());
            this.streams.get(tableName).put(streamName, stream);
            this.currentStreams.put(tableName, streamName);
        }
        if (oldStream != null) {
            commitStreamIfEligible(tableName, oldStream);
        }

        return true;
    }

    /**
     * This takes care of actually making the data available for viewing in BigQuery
     *
     * @param stream The stream which should be committed
     */
    private void finaliseAndCommitStream(ApplicationStream stream) {
        stream.finalise();
        stream.commit();
    }

    /**
     * Get or create stream for table
     *
     * @param tableName The table name
     * @return Current active stream on table
     */
    private String getCurrentStreamForTable(String tableName, List<Object[]> rows) {
        if (!currentStreams.containsKey(tableName)) {
            this.createStream(tableName, null, rows);
        }

        return Objects.requireNonNull(this.currentStreams.get(tableName));
    }

    /**
     * Commits the stream if it is not active and has written all the data assigned to it.
     *
     * @param tableName  The name of the table
     * @param streamName The name of the stream on table
     */
    private void commitStreamIfEligible(String tableName, String streamName) {
        if (!Objects.equals(currentStreams.get(tableName), streamName)) {
            logger.trace("Stream {} is not active, can be committed", streamName);
            ApplicationStream stream = this.streams.get(tableName).get(streamName);
            if (stream != null && stream.areAllExpectedCallsCompleted()) {
                // We are done with all expected calls for non-active streams, lets finalise and commit the stream.
                logger.trace("Stream {} has written all assigned offsets.", streamName);
                finaliseAndCommitStream(stream);
                logger.trace("Stream {} is now committed.", streamName);
                return;
            }
            logger.trace("Stream {} has not written all assigned offsets.", streamName);
        }
        logger.trace("Stream {} on table {} is not eligible for commit yet", streamName, tableName);
    }

    private Object lock(String tableName) {
        return tableLocks.computeIfAbsent(tableName, t -> new Object());
    }
}

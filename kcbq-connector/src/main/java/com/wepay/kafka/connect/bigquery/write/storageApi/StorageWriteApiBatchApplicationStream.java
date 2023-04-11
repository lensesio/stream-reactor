package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONArray;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class StorageWriteApiBatchApplicationStream extends StorageWriteApiApplicationStream {

    ConcurrentMap<String, LinkedHashMap<String, ApplicationStream>> streams;
    ConcurrentMap<String, String> currentStreams;

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
    }

    @Override
    public void shutdown() {
        this.streams.values()
                .stream().flatMap(item -> item.values().stream())
                .collect(Collectors.toList())
                .forEach(ApplicationStream::closeStream);
    }

    @Override
    public void appendRows(TableName tableName, List<Object[]> rows, String streamName) {
        JSONArray jsonArray = new JSONArray();
        rows.forEach(item -> jsonArray.put(item[1]));
        ApplicationStream applicationStream = this.streams.get(tableName.toString()).get(streamName);
        try {
            ApiFuture<AppendRowsResponse> response = applicationStream.writer().append(jsonArray);
            applicationStream.increaseAppendCallCount();
            ApiFutures.addCallback(response,
                    new StorageApiBatchCallbackHandler(
                            applicationStream, tableName.toString()), MoreExecutors.directExecutor());
        } catch (Exception e) {
            //TODO: Exception handling
            throw new BigQueryConnectException(e);
        }
    }

    private ApplicationStream createApplicationStream(String tableName) {
        try {
            return new ApplicationStream(tableName, getWriteClient());
        } catch (Exception e) {
            // PERMISSION_DENIED: Permission 'TABLES_UPDATE_DATA' denied on resource
            // 'projects/cloud-private-dev/datasets/bgoyal_march_01/tables/test503' (or it may not exist).
            // TODO: Table creation
            //TODO: Exception handling
            throw new BigQueryStorageWriteApiConnectException(e.getMessage());
        }

    }

    public boolean createStream(String tableName, String oldStream) {
        synchronized (this) {
            if (!Objects.equals(oldStream, this.currentStreams.get(tableName))) {
                return false;
            }
            // Current state is same as calling state. Create new Stream
            ApplicationStream stream = createApplicationStream(tableName);
            String streamName = stream.getStreamName();

            this.streams.computeIfAbsent(tableName, t -> new LinkedHashMap<>());
            this.streams.get(tableName).put(streamName, stream);
            this.currentStreams.put(tableName, streamName);
        }
        if (oldStream != null)
            commitStreamIfEligible(tableName, oldStream);

        return true;
    }

    public void finaliseAndCommitStream(ApplicationStream stream) {
        stream.finalise();
        stream.commit();
    }

    @Override
    public String getCurrentStreamForTable(String tableName) {
        if (!currentStreams.containsKey(tableName)) {
            this.createStream(tableName, null);
        }

        return Objects.requireNonNull(this.currentStreams.get(tableName));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetsReadyForCommits = new ConcurrentHashMap<>();
        streams.values()
                .forEach(map -> {
                            int i = 0;
                            for (ApplicationStream applicationStream : map.values()) {
                                i++;
                                logger.info("StreamName " + applicationStream.getStreamName() + ", all expected calls done ? " + applicationStream.areAllExpectedCallsCompleted());
                                if (applicationStream.isInactive()) {
                                    continue;
                                }
                                if (applicationStream.isReadyForOffsetCommit()) {
                                    offsetsReadyForCommits.putAll(applicationStream.getOffsetInformation());
                                    applicationStream.markInactive();
                                    logger.info("################### committed (item " + i + " in list out of " + map.size() + " items), Stream info " + applicationStream);
                                } else {
                                    logger.info("################### Not ready for commit (item " + i + " in list out of " + map.size() + " items), Stream info " + applicationStream);
                                    // We move sequentially for offset commit, until current offsets are ready, we cannot commit next.
                                    break;
                                }

                            }
                        }
                );
        logger.info("Batch was called to return commitableOffsets : " + offsetsReadyForCommits);
        return offsetsReadyForCommits;
    }


    @Override
    public boolean mayBeCreateStream(String tableName) {
        boolean result;
        synchronized (this) {
            String streamName = this.currentStreams.get(tableName);
            result = (streamName == null) || this.streams.get(tableName).get(streamName).canBeMovedToInactive();
            if (result) {
                return this.createStream(tableName, streamName);
            }
            return false;
        }
    }

    @Override
    public void updateOffsetsForStream(String tableName, String streamName, Map<TopicPartition, OffsetAndMetadata> offsetInfo) {
        this.streams.get(tableName).get(streamName).updateOffsetInformation(offsetInfo);
    }

    private void commitStreamIfEligible(String tableName, String streamName) {
        if (!currentStreams.getOrDefault(tableName, "").equals(streamName)) {
            // We are done with all expected calls for non-active streams, lets finalise and commit the stream.
            ApplicationStream stream = this.streams.get(tableName).get(streamName);
            if (stream.areAllExpectedCallsCompleted()) {
                finaliseAndCommitStream(stream);
                logger.debug("Stream {} on table {} is not eligible for commit yet", streamName, tableName);
                return;
            }
        }
        logger.debug("Stream {} on table {} is not eligible for commit yet", streamName, tableName);
    }

    public class StorageApiBatchCallbackHandler implements ApiFutureCallback<AppendRowsResponse> {

        private final ApplicationStream stream;
        private final String tableName;

        StorageApiBatchCallbackHandler(ApplicationStream stream, String tableName) {
            this.stream = stream;
            this.tableName = tableName;
        }

        @Override
        public void onFailure(Throwable t) {
            //TODO: Exception handling
            logger.info(t.getMessage());
            throw new BigQueryStorageWriteApiConnectException(t.getMessage());
        }

        @Override
        public void onSuccess(AppendRowsResponse result) {
            logger.info("Stream {} append call passed", stream.getStreamName());
            stream.increaseCompletedCallsCount();
            commitStreamIfEligible(tableName, stream.getStreamName());
        }
    }
}

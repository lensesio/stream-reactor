package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;

public class StorageApiBatchModeHandlerTest {
    StorageWriteApiApplicationStream mockedStreamApi = mock(StorageWriteApiApplicationStream.class);
    BigQuerySinkTaskConfig mockedConfig = mock(BigQuerySinkTaskConfig.class);
    Map<TopicPartition, OffsetAndMetadata> offsetInfo = new HashMap<>();
    StorageApiBatchModeHandler batchModeHandler = new StorageApiBatchModeHandler(
            mockedStreamApi,
            mockedConfig
    );
    List<Object[]> rows = new ArrayList<>();

    @Before
    public void setup() {
        when(mockedConfig.getString(BigQuerySinkTaskConfig.PROJECT_CONFIG)).thenReturn("p");
        when(mockedConfig.getString(BigQuerySinkTaskConfig.DEFAULT_DATASET_CONFIG)).thenReturn("d1");
        when(mockedConfig.getBoolean(BigQuerySinkTaskConfig.SANITIZE_TOPICS_CONFIG)).thenReturn(false);
        when(mockedConfig.getList(BigQuerySinkTaskConfig.TOPICS_CONFIG)).thenReturn(
                Arrays.asList("topic1", "topic2")
        );
        when(mockedStreamApi.mayBeCreateStream(any(), any())).thenReturn(true);
        when(mockedStreamApi.updateOffsetsOnStream(any(), any())).thenReturn("s1_app_stream");
        when(mockedStreamApi.getCommitableOffsets()).thenReturn(offsetInfo);
    }

    @Test
    public void testCreateStreams() {
        batchModeHandler.createNewStream();
    }

    @Test
    public void testUpdateOffsetsOnStream() {
        String actualStreamName = batchModeHandler.updateOffsetsOnStream(
                TableName.of("p", "d1", "topic1").toString(), rows);

        Assert.assertEquals("s1_app_stream", actualStreamName);
        verify(mockedStreamApi, times(1))
                .updateOffsetsOnStream("projects/p/datasets/d1/tables/topic1", rows);
    }

    @Test
    public void testGetCommitableOffsets() {
        batchModeHandler.getCommitableOffsets();
        verify(mockedStreamApi, times(1)).getCommitableOffsets();
    }
}

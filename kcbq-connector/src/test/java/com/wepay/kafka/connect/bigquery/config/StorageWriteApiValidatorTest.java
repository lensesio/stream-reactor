package com.wepay.kafka.connect.bigquery.config;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DELETE_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.UPSERT_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static com.wepay.kafka.connect.bigquery.config.StorageWriteApiValidator.legacyBatchNotSupportedError;
import static com.wepay.kafka.connect.bigquery.config.StorageWriteApiValidator.upsertNotSupportedError;
import static com.wepay.kafka.connect.bigquery.config.StorageWriteApiValidator.deleteNotSupportedError;
import static com.wepay.kafka.connect.bigquery.config.StorageWriteApiValidator.newBatchNotSupportedError;
import java.util.Collections;
import java.util.Optional;

public class StorageWriteApiValidatorTest {

    @Test
    public void testNoStorageWriteApiEnabled() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);

        assertEquals(Optional.empty(), new StorageWriteApiValidator().doValidate(config));
    }

    @Test
    public void testNoLegacyModesEnabled() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
        when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(false);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

        assertEquals(Optional.empty(), new StorageWriteApiValidator().doValidate(config));
    }

    @Test
    public void testUpsertModeEnabled() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(true);
        when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(false);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

        assertEquals(
                Optional.of(upsertNotSupportedError),
                new StorageWriteApiValidator().doValidate(config));
    }

    @Test
    public void testDeleteModeEnabled() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
        when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(true);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

        assertEquals(Optional.of(deleteNotSupportedError), new StorageWriteApiValidator().doValidate(config));
    }

    @Test
    public void testLegacyBatchModeEnabled() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
        when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(false);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("abc"));

        assertEquals(Optional.of(legacyBatchNotSupportedError), new StorageWriteApiValidator().doValidate(config));
    }

    @Test
    public void testNewBatchModeEnabledWithoutNewApi() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);
        when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);

        assertEquals(Optional.of(newBatchNotSupportedError),
                new StorageWriteApiValidator.StorageWriteApiBatchValidator().doValidate(config));
    }

    @Test
    public void testNewBatchModeEnabledWithNewApi() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);

        assertEquals(Optional.empty(),
                new StorageWriteApiValidator.StorageWriteApiBatchValidator().doValidate(config));
    }

    @Test
    public void testBothLegacyAndNewBatchEnabledOldApi() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);
        when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("abc"));

        assertEquals(Optional.of(newBatchNotSupportedError),
                new StorageWriteApiValidator.StorageWriteApiBatchValidator().doValidate(config));
    }

    @Test
    public void testBothLegacyAndNewBatchEnabledNewApi() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("abc"));

        assertEquals(Optional.of(legacyBatchNotSupportedError), new StorageWriteApiValidator().doValidate(config));
    }
}


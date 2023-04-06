package com.wepay.kafka.connect.bigquery.config;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.UPSERT_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DELETE_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;

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
                Optional.of(
                        "Upsert mode is not supported with Storage Write API." +
                                " Either disable Upsert mode or disable Storage Write API"),
                new StorageWriteApiValidator().doValidate(config));
    }

    @Test
    public void testDeleteModeEnabled() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
        when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(true);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

        assertEquals(Optional.of(
                        "Delete mode is not supported with Storage Write API." +
                                " Either disable Delete mode or disable Storage Write API"),
                new StorageWriteApiValidator().doValidate(config));
    }

    @Test
    public void testLegacyBatchModeEnabled() {
        BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

        when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
        when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
        when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(false);
        when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("abc"));

        assertEquals(Optional.of(
                        "Legacy Batch mode is not supported with Storage Write API." +
                                " Either disable Legacy Batch mode or disable Storage Write API"),
                new StorageWriteApiValidator().doValidate(config));
    }
}


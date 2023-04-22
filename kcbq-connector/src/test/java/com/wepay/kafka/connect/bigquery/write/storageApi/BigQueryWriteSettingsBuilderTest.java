package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class BigQueryWriteSettingsBuilderTest {


    @Test
    public void testBigQueryWriteSettingsBuild() {
        HeaderProvider expectedHeader = FixedHeaderProvider.create(
                "user-agent", "Confluent Platform (GPN: Confluent;) Google BigQuery Sink/unknown");

        BigQueryWriteSettings actualSettings = new BigQueryWriteSettingsBuilder().withConfig(getConfig()).build();

        assertEquals(actualSettings.getQuotaProjectId(), "abcd");
        assertEquals(actualSettings.getHeaderProvider(), expectedHeader);
    }

    private BigQuerySinkConfig getConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "abcd");
        properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
        properties.put(
                BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_CONFIG, BigQuerySinkConfig.CONNECTOR_RUNTIME_PROVIDER_DEFAULT);
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dummy_dataset");

        return new BigQuerySinkConfig(properties);
    }
}

package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.write.storageApi.BigQueryWriteSettingsBuilder;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@Category(IntegrationTest.class)
public class GcpClientBuilderIT extends BaseConnectorIT {

    private BigQuerySinkConfig connectorProps(GcpClientBuilder.KeySource keySource) throws IOException {
        Map<String, String> properties = baseConnectorProps(1);
        properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, keySource.name());

        if (keySource == GcpClientBuilder.KeySource.APPLICATION_DEFAULT) {
            properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, null);
        }
        else if (keySource == GcpClientBuilder.KeySource.JSON){
            // actually keyFile is the path to the credentials file, so we convert it to the json string
            String credentialsJsonString = new String(Files.readAllBytes(Paths.get(keyFile())), StandardCharsets.UTF_8);
            properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, credentialsJsonString);
        }

        return new BigQuerySinkConfig(properties);
    }

    /**
     * Construct the BigQuery and Storage clients and perform some basic operations to check they are operational.
     * @param keySource the key Source to use
     * @throws IOException
     */
    private void testClients(GcpClientBuilder.KeySource keySource) throws IOException {
        BigQuerySinkConfig config = connectorProps(keySource);

        BigQuery bigQuery = new GcpClientBuilder.BigQueryBuilder().withConfig(config).build();
        Storage storage = new GcpClientBuilder.GcsBuilder().withConfig(config).build();

        BigQueryWriteSettings settings = new BigQueryWriteSettingsBuilder().withConfig(config).build();
        BigQueryWriteClient client = BigQueryWriteClient.create(settings);
        TableId tableId = TableId.of(project(),dataset(),suffixedTableOrTopic("suthenticate-storage-api"));

        bigQuery.listTables(DatasetId.of(dataset()));
        storage.get(gcsBucket());

        bigQuery.create(TableInfo.of(tableId, StandardTableDefinition.newBuilder().build()));
        client.createWriteStream(tableId.toString(), WriteStream.newBuilder().build());
        bigQuery.delete(tableId);
    }

    @Test
    public void testApplicationDefaultCredentials() throws IOException {
        testClients(GcpClientBuilder.KeySource.APPLICATION_DEFAULT);
    }

    @Test
    public void testFile() throws IOException {
        testClients(GcpClientBuilder.KeySource.FILE);
    }

    @Test
    public void testJson() throws IOException {
        testClients(GcpClientBuilder.KeySource.JSON);
    }

}

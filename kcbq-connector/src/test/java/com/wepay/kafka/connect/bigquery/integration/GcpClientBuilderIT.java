package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.storage.Storage;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.storageApi.BigQueryWriteSettingsBuilder;
import org.apache.kafka.test.IntegrationTest;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class GcpClientBuilderIT extends BaseConnectorIT {

    Logger logger= LoggerFactory.getLogger(GcpClientBuilderIT.class);
    private BigQuery bigQueryInstance;

    private TableId tableId;
    @Before
    public void setup() throws Exception {
        bigQueryInstance = newBigQuery();
        tableId = TableId.of(project(),dataset(),"authenticate-storage-api");
        if(bigQueryInstance.getTable(tableId) == null) {
            logger.info("Going to Create table : "+ tableId.toString());
            bigQueryInstance.create(TableInfo.of(tableId, StandardTableDefinition.newBuilder().build()));
            logger.info("Created table : "+ tableId.toString());
            // table takes time after creation before being available for operations. You may have to wait a few minutes (~5 minutes)
            // Try to wait for 5 minutes if table is seen.
            int attempts = 10;
            while(bigQueryInstance.getTable(tableId) == null && attempts > 0)  {
                logger.debug("Busy waiting for table {} to appear! Attempt {}", tableId.getTable(), (10-attempts));
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                attempts--;
            }
            if(attempts == 0) {
                throw new AssertionError(
                        "Created table is not yet available. Re-run test after a few minutes");
            }
        }
    }

    private Map<String, String> connectorProps(GcpClientBuilder.KeySource keySource) throws IOException {
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

        return properties;
    }

    /**
     * Construct the BigQuery and Storage clients and perform some basic operations to check they are operational.
     * @param keySource the key Source to use
     * @throws IOException
     */
    private void testClients(GcpClientBuilder.KeySource keySource) throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        Map<String, String> properties = connectorProps(keySource);
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "false");
        BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

        BigQuery bigQuery = new GcpClientBuilder.BigQueryBuilder().withConfig(config).build();
        Storage storage = new GcpClientBuilder.GcsBuilder().withConfig(config).build();

        BigQueryWriteSettings settings = new BigQueryWriteSettingsBuilder().withConfig(config).build();
        BigQueryWriteClient client = BigQueryWriteClient.create(settings);

        bigQuery.listTables(DatasetId.of(dataset()));
        storage.get(gcsBucket());
        JsonStreamWriter writer = JsonStreamWriter.newBuilder(TableNameUtils.tableName(tableId).toString(), client).build();
        assertTrue(writer.getStreamName().contains("default"));
    }

    private void testClientsWithScopes(GcpClientBuilder.KeySource keySource) throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        Map<String, String> properties = connectorProps(keySource);
        properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
        BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

        BigQuery bigQuery = new GcpClientBuilder.BigQueryBuilder().withConfig(config).build();
        Storage storage = new GcpClientBuilder.GcsBuilder().withConfig(config).build();

        BigQueryWriteSettings settings = new BigQueryWriteSettingsBuilder().withConfig(config).build();
        BigQueryWriteClient client = BigQueryWriteClient.create(settings);

        bigQuery.listTables(DatasetId.of(dataset()));
        storage.get(gcsBucket());

        JsonStreamWriter writer = JsonStreamWriter.newBuilder(TableNameUtils.tableName(tableId).toString(), client).build();
        assertTrue(writer.getStreamName().contains("default"));
    }

    @Test
    public void testApplicationDefaultCredentials() throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        testClients(GcpClientBuilder.KeySource.APPLICATION_DEFAULT);
        testClientsWithScopes(GcpClientBuilder.KeySource.APPLICATION_DEFAULT);
    }

    @Test
    public void testFile() throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        testClients(GcpClientBuilder.KeySource.FILE);
        testClientsWithScopes(GcpClientBuilder.KeySource.FILE);
    }

    @Test
    public void
    testJson() throws IOException, Descriptors.DescriptorValidationException, InterruptedException {
        testClients(GcpClientBuilder.KeySource.JSON);
        testClientsWithScopes(GcpClientBuilder.KeySource.JSON);
    }

}

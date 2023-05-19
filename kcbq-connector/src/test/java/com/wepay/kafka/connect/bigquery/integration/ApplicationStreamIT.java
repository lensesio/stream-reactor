package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.storageApi.ApplicationStream;
import com.wepay.kafka.connect.bigquery.write.storageApi.BigQueryWriteSettingsBuilder;
import com.wepay.kafka.connect.bigquery.write.storageApi.StreamState;
import org.apache.kafka.connect.errors.ConnectException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;


public class ApplicationStreamIT extends BaseConnectorIT {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationStreamIT.class);
    private BigQuery bigQuery;
    String table = "applicationStreamTest";
    TableName tableName = TableName.of(project(), dataset(), table);
    String tableNameStr = tableName.toString();
    BigQueryWriteClient client;
    BigQueryWriteSettings writeSettings;

    @Before
    public void setup() throws IOException, InterruptedException {
        bigQuery = newBigQuery();
        createTable();
        writeSettings = new BigQueryWriteSettingsBuilder()
                .withProject(project())
                .withKeySource(GcpClientBuilder.KeySource.valueOf(keySource()))
                .withKey(keyFile())
                .withUserAgent("IT-Test-Application-Stream")
                .withWriterApi(true)
                .build();
        client = BigQueryWriteClient.create(writeSettings);
    }

    @Test
    public void testStreamCreation() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        assertEquals(applicationStream.getCurrentState(), StreamState.CREATED);
        assertNotNull(applicationStream.writer());
        applicationStream.closeStream();
    }

    @Test
    public void testStreamClose() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        String streamName = applicationStream.writer().getStreamName();
        applicationStream.closeStream();
        assertNotEquals(applicationStream.writer().getStreamName(), streamName);
    }

    @Test
    public void testApplicationStreamName() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        assertTrue(applicationStream.getStreamName().contains("streams"));
        applicationStream.closeStream();
    }

    @Test
    public void testMaxCallCount() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        assertEquals(applicationStream.getCurrentState(), StreamState.CREATED);
        int maxCount = applicationStream.increaseMaxCalls();
        assertEquals(applicationStream.getCurrentState(), StreamState.APPEND);
        assertEquals(1, maxCount);
        applicationStream.closeStream();
    }

    @Test
    public void testCanBeMovedToNonActive() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        assertFalse(applicationStream.canTransitionToNonActive());
        applicationStream.increaseMaxCalls();
        assertTrue(applicationStream.canTransitionToNonActive());
        applicationStream.closeStream();
    }

    @Test
    public void testResetWriter() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        JsonStreamWriter writer = applicationStream.writer();
        applicationStream.closeStream();
        JsonStreamWriter updatedWriter = applicationStream.writer();
        assertNotEquals(writer, updatedWriter);
        applicationStream.closeStream();
    }

    @Test
    public void testStreamFinalised() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        applicationStream.increaseMaxCalls();
        applicationStream.closeStream();
        applicationStream.writer();
        assertEquals(applicationStream.getCurrentState(), StreamState.APPEND);
        applicationStream.finalise();
        assertEquals(applicationStream.getCurrentState(), StreamState.FINALISED);
        applicationStream.closeStream();
    }

    @Test
    public void testStreamCommitted() throws Exception {
        ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client);
        applicationStream.increaseMaxCalls();
        applicationStream.closeStream();
        applicationStream.writer();
        applicationStream.finalise();
        assertEquals(applicationStream.getCurrentState(), StreamState.FINALISED);
        applicationStream.commit();
        assertEquals(applicationStream.getCurrentState(), StreamState.COMMITTED);
        applicationStream.closeStream();
    }

    private void createTable() throws InterruptedException {
        try {
            BigQueryTestUtils.createPartitionedTable(bigQuery, dataset(), table, null);
            int attempts = 10;
            while (bigQuery.getTable(TableNameUtils.tableId(tableName)) == null && attempts > 0) {
                logger.debug("Busy waiting for table {} to appear! Attempt {}", table, (10 - attempts));
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                attempts--;
            }
        } catch (BigQueryException ex) {
            if (!ex.getError().getReason().equalsIgnoreCase("duplicate"))
                throw new ConnectException("Failed to create table: ", ex);
            else
                logger.info("Table {} already exist", table);
        }
    }
}

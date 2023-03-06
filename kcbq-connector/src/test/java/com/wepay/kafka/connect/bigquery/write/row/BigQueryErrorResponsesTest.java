package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQueryException;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class BigQueryErrorResponsesTest {

    @Test
    public void testIsAuthenticationError() {
        BigQueryException error = new BigQueryException(0, "......401.....Unauthorized error.....");
        assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......401.....Unauthorized error...invalid_grant..");
        assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......400........invalid_grant..");
        assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......400.....invalid_request..");
        assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......400.....invalid_client..");
        assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......400.....unauthorized_client..");
        assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......400.....unsupported_grant_type..");
        assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......403..Access denied error.....");
        assertFalse(BigQueryErrorResponses.isAuthenticationError(error));

        error = new BigQueryException(0, "......500...Internal Server Error...");
        assertFalse(BigQueryErrorResponses.isAuthenticationError(error));
    }
}

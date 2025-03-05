package com.wepay.kafka.connect.bigquery.filter;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
public class GcpCredsFilterTest {

    @Test
    public void testFilterCredsConfig() {

        String keyfile = "{\n  \"type\": \"service_account\",\n  \"project_id\": \"connect\",\n  \"private_key_id\": \"adjshd\",\n  \"private_key\": \"test-key\",\n  \"client_email\": \"test-bigquery-playground\",\n  \"client_id\": \"1027\",\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com\",\n  \"universe_domain\": \"googleapis.com\"\n}\n";
        String expectedKeyfileConfig = "{\n  \"type\" : \"service_account\",\n  \"project_id\" : \"connect\",\n  \"private_key_id\" : \"adjshd\",\n  \"private_key\" : \"test-key\",\n  \"client_email\" : \"test-bigquery-playground\",\n  \"client_id\" : \"1027\"\n}\n".trim();

        assertEquals(expectedKeyfileConfig, GcpCredsFilter.filterCreds(keyfile, false));
    }

    @Test
    public void testFilterCredsFile() {

        String keyfile = "src/test/resources/keyfile.json";
        String expectedKeyfileConfig = "{\n  \"type\" : \"service_account\",\n  \"project_id\" : \"connect\",\n  \"private_key_id\" : \"adjshd\",\n  \"private_key\" : \"test-key\",\n  \"client_email\" : \"test-bigquery-playground\",\n  \"client_id\" : \"1027\"\n}\n".trim();

        assertEquals(expectedKeyfileConfig, GcpCredsFilter.filterCreds(keyfile, true));
    }

    @Test(expected = BigQueryConnectException.class)
    public void testFilterCredsConfigWithException() {

        // The keyfile string is not a correct JSON
        String keyfile = "{\n  \"type\": \"service_account\", project_id\": \"connect\" private_key_id\" \"adjshd\",\n  \"private_key\": \"test-key\",\n  \"client_email\": \"test-bigquery-playground\",\n  \"client_id\": \"1027\",\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com\",\n  \"universe_domain\": \"googleapis.com\"\n}\n";
        GcpCredsFilter.filterCreds(keyfile, false);

    }

    @Test(expected = BigQueryConnectException.class)
    public void testEmptyFilteredCredsConfig() {

        // This keyfile will be modified to an empty object after filtering fields
        // since none of these fields are allowed to pass the filter
        String keyfile = "{\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com\",\n  \"universe_domain\": \"googleapis.com\"\n}\n";
        GcpCredsFilter.filterCreds(keyfile, false);

    }
}

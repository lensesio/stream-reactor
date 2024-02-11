package com.wepay.kafka.connect.bigquery.filter;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

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
}

/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package com.wepay.kafka.connect.bigquery.filter;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

public class GcpCredsFilter {

    // Fields allowed to pass filtering
    private static final String[] allowedFields = new String[]
            {"type", "project_id", "private_key_id",
                    "private_key", "client_email", "client_id"};


    /**
     * This accepts the credentials config specified in the connector
     * and returns a byte array with filtered configs.
     * <p>Here creds config itself holds the credentials JSON string or path to it.
     * @param credsConfig Creds config file path or the stringified credentials
     * @param isFilePath Boolean to find if the credsConfig points to file path
     * @return string with filtered creds
     */
    public static String filterCreds(String credsConfig, boolean isFilePath) {
        try {
            JsonNode keyfileNode;

            if(isFilePath) {
                keyfileNode = new ObjectMapper().readTree(new File(credsConfig));
            } else {
                keyfileNode = new ObjectMapper().readTree(credsConfig);
            }

            Iterator<Map.Entry<String, JsonNode>> fieldsIterator = keyfileNode.fields();
            Set<String> fields = new HashSet<>(Arrays.asList(allowedFields));

            while (fieldsIterator.hasNext()) {
                Map.Entry<String, JsonNode> field = fieldsIterator.next();
                String fieldName = field.getKey();
                if (!fields.contains(fieldName)) {
                    fieldsIterator.remove();
                }
            }

            // After filtering, any empty keyfile node will be translated as "{}" string
            // which is non-empty. That's why we check whether the node is itself empty.
            if(keyfileNode.size() == 0) {
                throw new BigQueryConnectException("The keyfile does not contain valid fields. Please recheck the keyfile config.");
            }

            return new ObjectMapper().writer()
                    .withDefaultPrettyPrinter().writeValueAsString(keyfileNode);
        } catch (Exception e) {
            throw new BigQueryConnectException("Failed to access Keyfile config: ", e);
        }
    }
}
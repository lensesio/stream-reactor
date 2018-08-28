package com.wepay.kafka.connect.bigquery;

/*
 * Copyright 2016 WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import com.wepay.kafka.connect.bigquery.exception.GCSConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Convenience class for creating a {@link com.google.cloud.storage.Storage} instance
 */
public class GCSBuilder {
  private static final Logger logger = LoggerFactory.getLogger(GCSBuilder.class);

  private final String projectName;
  private String keyFileName;

  public GCSBuilder(String projectName) {
    this.projectName = projectName;
    this.keyFileName = null;
  }

  public GCSBuilder setKeyFileName(String keyFileName) {
    this.keyFileName = keyFileName;
    return this;
  }

  public Storage build() {
    return connect(projectName, keyFileName);
  }

  /**
   * Returns a default {@link Storage} instance for the specified project with credentials provided
   * in the specified file.
   *
   * @param projectName The name of the GCS project to work with
   * @param keyFilename The name of a file containing a JSON key that can be used to provide
   *                    credentials to GCS, or null if no authentication should be performed.
   * @return The resulting Storage object.
   */
  private Storage connect(String projectName, String keyFilename) {
    if (keyFilename == null) {
      return connect(projectName);
    }

    logger.debug("Attempting to open file {} for service account json key", keyFilename);
    try (InputStream credentialsStream = new FileInputStream(keyFilename)) {
      logger.debug("Attempting to authenticate with GCS using provided json key");
      return StorageOptions.newBuilder()
          .setProjectId(projectName)
          .setCredentials(GoogleCredentials.fromStream(credentialsStream))
          .build()
          .getService();
    } catch (IOException err) {
      throw new GCSConnectException("Failed to access json key file", err);
    }
  }

  /**
   * Returns a default {@link Storage} instance for the specified project with no authentication
   * credentials.
   *
   * @param projectName The name of the GCS project to work with
   * @return The resulting Storage object.
   */
  private Storage connect(String projectName) {
    logger.debug("Attempting to access BigQuery without authentication");
    return StorageOptions.newBuilder()
        .setProjectId(projectName)
        .build()
        .getService();
  }
}

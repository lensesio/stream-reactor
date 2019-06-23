package com.wepay.kafka.connect.bigquery.it.utils;

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


import com.google.cloud.storage.Bucket;

import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.GCSBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketClearer {

  private static final Logger logger = LoggerFactory.getLogger(BucketClearer.class);

  /**
   * Clears tables in the given project and dataset, using a provided JSON service account key.
   */
  public static void main(String[] args) {
    if (args.length < 3) {
      usage();
    }

    Storage gcs = new GCSBuilder(args[1]).setKeyFileName(args[0]).build();

    // if bucket exists, delete it.
    String bucketName = args[2];
    if (gcs.delete(bucketName)) {
      logger.info("Bucket {} deleted successfully", bucketName);
    } else {
      logger.info("Bucket {} does not exist", bucketName);
    }
  }

  private static void usage() {
    System.err.println(
        "usage: BucketClearer <key_file> <project_name> <bucket_name>"
    );
    System.exit(1);
  }
}

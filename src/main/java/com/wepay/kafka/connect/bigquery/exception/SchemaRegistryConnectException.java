package com.wepay.kafka.connect.bigquery.exception;

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


import org.apache.kafka.connect.errors.ConnectException;

/**
 * Class for exceptions that occur while interacting with the Schema Registry, such as failures to
 * retrieve table information and failure to provide a valid SchemaRegistryClient.
 */
public class SchemaRegistryConnectException extends ConnectException {
  public SchemaRegistryConnectException(String msg) {
    super(msg);
  }

  public SchemaRegistryConnectException(String msg, Throwable thr) {
    super(msg, thr);
  }

  public SchemaRegistryConnectException(Throwable thr) {
    super(thr);
  }
}

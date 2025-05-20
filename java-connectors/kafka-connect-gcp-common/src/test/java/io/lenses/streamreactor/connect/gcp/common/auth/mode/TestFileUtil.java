/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.gcp.common.auth.mode;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.google.common.io.ByteStreams;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestFileUtil {

  static String streamToString(InputStream inputStream) throws Exception {
    byte[] bytes = ByteStreams.toByteArray(inputStream);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  static String resourceAsString(String resourceFile) throws Exception {
    return streamToString(TestFileUtil.class.getResourceAsStream(resourceFile));
  }

  static String absolutePathForResource(String resourceName) {
    URL resourceUrl = TestFileUtil.class.getResource(resourceName);
    checkNotNull(resourceUrl, String.format("Resource not found: %s", resourceName));
    return new File(resourceUrl.getFile()).getAbsolutePath();
  }
}

/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.aws.s3.auth
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import software.amazon.awssdk.services.s3.S3Client

trait AuthResource[R] {

  def create(awsConfig: S3Config): Either[String, R]

}

class AuthResources(awsConfig: S3Config) {

  def aws: Either[String, S3Client] = AwsAuthResourceCreator.create(awsConfig)

}

object AwsAuthResourceCreator extends AuthResource[S3Client] {

  override def create(awsConfig: S3Config): Either[String, S3Client] =
    new AwsS3ClientCreator(awsConfig).createS3Client()

}

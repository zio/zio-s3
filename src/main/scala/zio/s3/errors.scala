/*
 * Copyright 2017-2020 John A. De Goes and the ZIO Contributors
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

package zio.s3

import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.s3.model.S3Exception

final case class SdkError(error: SdkException)
  extends S3Exception(S3Exception.builder().message(error.getMessage).cause(error))

final case class InvalidCredentials(message: String) extends S3Exception(S3Exception.builder().message(message))

final case class InvalidSettings(message: String) extends S3Exception(S3Exception.builder().message(message))

final case class ConnectionError(message: String, cause: Throwable)
    extends S3Exception(S3Exception.builder().message(message))

final case class InvalidPartSize(message: String, size: Int) extends S3Exception(S3Exception.builder().message(message))

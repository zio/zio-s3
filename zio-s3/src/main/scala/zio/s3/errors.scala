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

import software.amazon.awssdk.core.exception.{ SdkException, SdkServiceException }
import software.amazon.awssdk.services.s3.model.S3Exception
import zio.Cause

import java.nio.charset.CharacterCodingException
import scala.util.control.NonFatal

object errors {

  final case class SdkError(error: SdkException)
      extends S3Exception(S3Exception.builder().message(error.getMessage).cause(error))

  final case class InvalidCredentials(message: String) extends S3Exception(S3Exception.builder().message(message))

  final case class InvalidSettings(message: String) extends S3Exception(S3Exception.builder().message(message))

  final case class ConnectionError(message: String, cause: Throwable)
      extends S3Exception(S3Exception.builder().message(message))

  final case class InvalidPartSize(message: String, size: Int)
      extends S3Exception(S3Exception.builder().message(message))

  final case class DecodingException(cause: CharacterCodingException)
      extends S3Exception(S3Exception.builder().cause(cause))

  object syntax {

    implicit class S3ExceptionOps(ex: Throwable) {

      def asS3Exception(): Cause[S3Exception] =
        ex match {
          case e: SdkServiceException =>
            Cause.fail(
              S3Exception
                .builder()
                .statusCode(e.statusCode())
                .requestId(e.requestId())
                .message(e.getMessage)
                .cause(e)
                .build()
                .asInstanceOf[S3Exception]
            )
          case NonFatal(e)            =>
            Cause.fail(S3Exception.builder().message(e.getMessage).cause(e).build().asInstanceOf[S3Exception])
          case other                  => Cause.die(other)
        }
    }
  }
}

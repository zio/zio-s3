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

/**
 * The errors of the `S3` service. Each one extends `S3Exception`, so a caller can
 * handle them with the same error type as the errors of the AWS service.
 */
object errors {

  /**
   * An AWS SDK error that is not an S3 service error, such as a client-side failure.
   * Wraps the `SdkException`.
   *
   * @param error the AWS SDK exception
   */
  final case class SdkError(error: SdkException)
      extends S3Exception(S3Exception.builder().message(error.getMessage).cause(error))

  /**
   * A credentials provider could not resolve credentials. The effects of `providers` that
   * check credentials (all but `const`, `basic`, `session` and `default`) fail with this
   * error.
   *
   * @param message the message of the provider failure
   */
  final case class InvalidCredentials(message: String) extends S3Exception(S3Exception.builder().message(message))

  /**
   * A setting is not valid, such as a region that is not in the AWS SDK region list.
   *
   * @param message description of the invalid setting
   */
  final case class InvalidSettings(message: String) extends S3Exception(S3Exception.builder().message(message))

  /**
   * The S3 client could not be built, or the effect that gives its credentials provider
   * failed.
   *
   * @param message the message of the failure
   * @param cause the cause of the failure, or `null` when the failure has no cause
   */
  final case class ConnectionError(message: String, cause: Throwable)
      extends S3Exception(S3Exception.builder().message(message).cause(cause))

  /**
   * Declared for an invalid part size. The library does not raise it: an invalid part
   * size makes `multipartUpload` die with a message instead.
   *
   * @param message description of the invalid part size
   * @param size the part size in bytes
   */
  final case class InvalidPartSize(message: String, size: Int)
      extends S3Exception(S3Exception.builder().message(message))

  /**
   * The bytes of an object are not valid UTF-8. `streamLines` fails with this error.
   *
   * @param cause the decoding failure
   */
  final case class DecodingException(cause: CharacterCodingException)
      extends S3Exception(S3Exception.builder().cause(cause))

  /**
   * Extension methods that convert throwables into `S3Exception` causes.
   */
  object syntax {

    /**
     * Converts a throwable into a `Cause` of an `S3Exception`.
     */
    implicit class S3ExceptionOps(ex: Throwable) {

      /**
       * Converts this throwable into a `Cause`. An `SdkServiceException`, which includes
       * `S3Exception` and the classes of the `errors` object, becomes a plain `S3Exception`
       * with the same status code, request ID and message. Another non-fatal throwable fails
       * with an `S3Exception` that has its message. A fatal one dies.
       */
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

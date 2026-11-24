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

import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region
import zio.{ IO, ZIO }
import zio.s3.errors._

/**
 * An AWS region for the `S3` service. Create one with `from`, or with
 * `unsafeFromString` for S3-compatible storage.
 *
 * @param region the AWS SDK region
 */
sealed abstract class S3Region(val region: Region)

/**
 * Constructors of `S3Region`.
 */
object S3Region { self =>

  /**
   * Returns the region as an `S3Region` when it is in the AWS SDK region list
   * (`Region.regions()`). Otherwise returns an `InvalidSettings` error with the
   * message `Invalid aws region provided : ` followed by the region ID.
   *
   * @param region the AWS SDK region to check
   */
  def from(region: Region): Either[InvalidSettings, S3Region] =
    region match {
      case r if Region.regions().contains(r) => Right(new S3Region(r) {})
      case r                                 => Left(InvalidSettings(s"Invalid aws region provided : ${r.id}"))
    }

  /**
   * Creates an `S3Region` from a region name without checking it against the AWS SDK
   * region list. Use it only for S3-compatible storage with a region that the AWS SDK
   * does not list, such as OVH.
   *
   * @param r region name, passed to `Region.of`
   */
  def unsafeFromString(r: String): S3Region =
    new S3Region(Region.of(r)) {}
}

/**
 * The settings of the `S3` service: the region, and the AWS credentials that sign
 * the requests.
 */
final case class S3Settings(s3Region: S3Region, credentials: AwsCredentials)

/**
 * Constructors of `S3Settings`.
 */
object S3Settings {

  /**
   * Builds the settings for `region` and `credentials`. The effect fails with an
   * `InvalidSettings` error when the region is not in the AWS SDK region list.
   *
   * @param region the AWS SDK region
   * @param credentials the AWS credentials
   */
  def from(region: Region, credentials: AwsCredentials): IO[InvalidSettings, S3Settings] =
    ZIO.fromEither(S3Region.from(region)).map(S3Settings(_, credentials))
}

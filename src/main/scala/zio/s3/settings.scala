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
import zio.blocking.{ Blocking, effectBlocking }
import zio.{ IO, Managed, UManaged, ZIO, ZManaged }

object CredentialsProviders {

  def const(accessKeyId: String, secretAccessKey: String): UManaged[AwsCredentialsProvider] =
    ZManaged.succeedNow[AwsCredentialsProvider](() => AwsBasicCredentials.create(accessKeyId, secretAccessKey))

  val system: Managed[InvalidCredentials, SystemPropertyCredentialsProvider] =
    ZManaged
      .succeed(SystemPropertyCredentialsProvider.create())
      .tapM(c => ZIO(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  val env: Managed[InvalidCredentials, EnvironmentVariableCredentialsProvider] =
    ZManaged
      .succeed(EnvironmentVariableCredentialsProvider.create())
      .tapM(c =>
        ZIO
          .effect(c.resolveCredentials())
          .mapError(err => InvalidCredentials(err.getMessage))
      )

  val profile: ZManaged[Blocking, InvalidCredentials, ProfileCredentialsProvider] =
    ZManaged
      .fromAutoCloseable(IO.succeed(ProfileCredentialsProvider.create()))
      .tapM(c =>
        effectBlocking(c.resolveCredentials())
          .mapError(err => InvalidCredentials(err.getMessage))
      )

  val container: ZManaged[Blocking, InvalidCredentials, ContainerCredentialsProvider] =
    ZManaged
      .fromAutoCloseable(
        IO.succeed(
          ContainerCredentialsProvider
            .builder()
            .build()
        )
      )
      .tapM(c => effectBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  val instanceProfile: ZManaged[Blocking, InvalidCredentials, InstanceProfileCredentialsProvider] =
    ZManaged
      .fromAutoCloseable(
        IO.succeed(
          InstanceProfileCredentialsProvider
            .create()
        )
      )
      .tapM(c => effectBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  /**
   * Use of this layer requires the awssdk sts module to be on the classpath,
   * by default zio-s3 required this library
   */
  val webIdentity: ZManaged[Blocking, InvalidCredentials, WebIdentityTokenFileCredentialsProvider] =
    ZManaged
      .succeed(
        WebIdentityTokenFileCredentialsProvider
          .create()
      )
      .tapM(c => effectBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  /**
   * Use default chaining strategy to fetch credentials
   */
  val default: ZManaged[Blocking, InvalidCredentials, AwsCredentialsProvider] =
    ZManaged.fromAutoCloseable(
      IO.succeed(DefaultCredentialsProvider.create())
    )
}

final case class S3Region(region: Region)

object S3Region { self =>

  // need to declare an default constructor to make inaccessible default constructor of case class
  private def apply(region: Region): Either[InvalidSettings, S3Region] =
    region match {
      case r if Region.regions().contains(r) => Right(new S3Region(r))
      case r                                 => Left(InvalidSettings(s"Invalid aws region provided : ${r.id}"))
    }

  def from(region: Region): Either[InvalidSettings, S3Region] =
    self.apply(region)

  /**
   * Only use for supporting other region for different s3 compatible storage provider such as OVH
   * Your S3 region might be invalid and will result into runtime error.
   * @param s unsafe region
   */
  def unsafeFromString(r: String): S3Region =
    new S3Region(Region.of(r))
}

final case class S3Settings(s3Region: S3Region, credentials: AwsCredentials)

object S3Settings {

  def from(region: Region, credentials: AwsCredentials): IO[InvalidSettings, S3Settings] =
    ZIO.fromEither(S3Region.from(region)).map(S3Settings(_, credentials))
}

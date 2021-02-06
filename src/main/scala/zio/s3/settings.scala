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
import zio.{ IO, Managed, UIO, ZIO, ZManaged }

final case class S3Credentials(accessKeyId: String, secretAccessKey: String)

object S3Credentials {
  def apply(c: AwsCredentials): S3Credentials = S3Credentials(c.accessKeyId(), c.secretAccessKey())

  private[this] def load[R](
    provider: Managed[Throwable, AwsCredentialsProvider]
  )(f: AwsCredentialsProvider => ZIO[R, Throwable, AwsCredentials]): ZIO[R, InvalidCredentials, S3Credentials] =
    provider
      .use(f)
      .map(S3Credentials(_))
      .mapError(e => InvalidCredentials(e.getMessage))

  def const(accessKeyId: String, secretAccessKey: String): UIO[S3Credentials] =
    UIO.succeed(S3Credentials(accessKeyId, secretAccessKey))

  val fromSystem: IO[InvalidCredentials, S3Credentials] =
    load(ZManaged.succeed(SystemPropertyCredentialsProvider.create()))(p => IO(p.resolveCredentials()))

  val fromEnv: IO[InvalidCredentials, S3Credentials] =
    load(ZManaged.succeed(EnvironmentVariableCredentialsProvider.create()))(p => IO(p.resolveCredentials()))

  val fromProfile: ZIO[Blocking, InvalidCredentials, S3Credentials] =
    load(
      ZManaged.fromAutoCloseable(
        IO(
          ProfileCredentialsProvider
            .builder()
            .build()
        )
      )
    )(p => effectBlocking(p.resolveCredentials()))

  val fromContainer: ZIO[Blocking, InvalidCredentials, S3Credentials] =
    load(
      ZManaged.fromAutoCloseable(
        IO(
          ContainerCredentialsProvider
            .builder()
            .build()
        )
      )
    )(p => effectBlocking(p.resolveCredentials()))

  val fromInstanceProfile: ZIO[Blocking, InvalidCredentials, S3Credentials] =
    load(
      ZManaged.fromAutoCloseable(
        IO(
          InstanceProfileCredentialsProvider
            .builder()
            .build()
        )
      )
    )(p => effectBlocking(p.resolveCredentials()))

  /**
   * Use of this layer requires the awssdk sts module to be on the classpath,
   * by default zio-s3 required this library
   */
  val fromWebIdentity: ZIO[Blocking, InvalidCredentials, S3Credentials] =
    load(
      ZManaged.effect(WebIdentityTokenFileCredentialsProvider.create())
    )(p => effectBlocking(p.resolveCredentials()))

  val fromAll: ZIO[Blocking, InvalidCredentials, S3Credentials] =
    fromSystem <> fromEnv <> fromProfile <> fromContainer <> fromInstanceProfile <> fromWebIdentity
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
}

final case class S3Settings(s3Region: S3Region, credentials: S3Credentials)

object S3Settings {

  def from(region: Region, credentials: S3Credentials): IO[InvalidSettings, S3Settings] =
    ZIO.fromEither(S3Region.from(region)).map(S3Settings(_, credentials))
}

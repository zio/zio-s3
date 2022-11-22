package zio.s3

import software.amazon.awssdk.auth.credentials._
import zio.{ IO, Scope, UIO, ZIO }
import zio.s3.errors._

object providers {

  def const(credential: AwsCredentials): UIO[AwsCredentialsProvider] =
    ZIO.succeedNow[AwsCredentialsProvider](() => credential)

  def basic(accessKeyId: String, secretAccessKey: String): UIO[AwsCredentialsProvider] =
    const(AwsBasicCredentials.create(accessKeyId, secretAccessKey))

  def session(accessKeyId: String, secretAccessKey: String, sessionToken: String): UIO[AwsCredentialsProvider] =
    const(AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken))

  val system: IO[InvalidCredentials, SystemPropertyCredentialsProvider] =
    ZIO
      .succeed(SystemPropertyCredentialsProvider.create())
      .tap(c => ZIO.attemptBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  val env: IO[InvalidCredentials, EnvironmentVariableCredentialsProvider] =
    ZIO
      .succeed(EnvironmentVariableCredentialsProvider.create())
      .tap(c =>
        ZIO
          .attemptBlocking(c.resolveCredentials())
          .mapError(err => InvalidCredentials(err.getMessage))
      )

  val profile: ZIO[Scope, InvalidCredentials, ProfileCredentialsProvider] =
    profile(None)

  def profile(name: Option[String]): ZIO[Scope, InvalidCredentials, ProfileCredentialsProvider] =
    ZIO
      .fromAutoCloseable(ZIO.succeed(ProfileCredentialsProvider.create(name.orNull)))
      .tap(c =>
        ZIO
          .attemptBlocking(c.resolveCredentials())
          .mapError(err => InvalidCredentials(err.getMessage))
      )

  val container: ZIO[Scope, InvalidCredentials, ContainerCredentialsProvider] =
    ZIO
      .fromAutoCloseable(
        ZIO.succeed(
          ContainerCredentialsProvider
            .builder()
            .build()
        )
      )
      .tap(c => ZIO.attemptBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  val instanceProfile: ZIO[Scope, InvalidCredentials, InstanceProfileCredentialsProvider] =
    ZIO
      .fromAutoCloseable(
        ZIO.succeed(
          InstanceProfileCredentialsProvider
            .create()
        )
      )
      .tap(c => ZIO.attemptBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  /**
   * Use of this layer requires the awssdk sts module to be on the classpath,
   * by default zio-s3 required this library
   */
  val webIdentity: ZIO[Scope, InvalidCredentials, WebIdentityTokenFileCredentialsProvider] =
    ZIO
      .succeed(
        WebIdentityTokenFileCredentialsProvider
          .create()
      )
      .tap(c => ZIO.attemptBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  /**
   * Use default chaining strategy to fetch credentials
   */
  val default: ZIO[Scope, InvalidCredentials, AwsCredentialsProvider] =
    ZIO.fromAutoCloseable(
      ZIO.succeed(DefaultCredentialsProvider.create())
    )
}

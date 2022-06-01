package zio.s3

import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentials,
  AwsCredentialsProvider,
  ContainerCredentialsProvider,
  DefaultCredentialsProvider,
  EnvironmentVariableCredentialsProvider,
  InstanceProfileCredentialsProvider,
  ProfileCredentialsProvider,
  SystemPropertyCredentialsProvider,
  WebIdentityTokenFileCredentialsProvider
}
import zio.blocking.{ Blocking, effectBlocking }
import zio.{ IO, Managed, UManaged, ZIO, ZManaged }

object providers {

  def const(accessKeyId: String, secretAccessKey: String): UManaged[AwsCredentialsProvider] =
    ZManaged.succeedNow[AwsCredentialsProvider](new AwsCredentialsProvider {
      override def resolveCredentials(): AwsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey)
    })

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
    profile(None)

  def profile(name: Option[String]): ZManaged[Blocking, InvalidCredentials, ProfileCredentialsProvider] =
    ZManaged
      .fromAutoCloseable(IO.succeed(ProfileCredentialsProvider.create(name.orNull)))
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

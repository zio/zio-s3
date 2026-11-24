package zio.s3

import software.amazon.awssdk.auth.credentials._
import zio.{ IO, Scope, UIO, ZIO }
import zio.s3.errors._

/**
 * Effects that build AWS credentials providers for the `S3` layers. Each effect
 * checks that its provider can resolve credentials, except `const`, `basic`,
 * `session` and `default`.
 */
object providers {

  /**
   * Gives a provider that always returns `credential`.
   */
  def const(credential: AwsCredentials): UIO[AwsCredentialsProvider] =
    ZIO.succeed[AwsCredentialsProvider](() => credential)

  /**
   * Gives a provider with fixed basic credentials, from an access key ID and a secret
   * access key.
   */
  def basic(accessKeyId: String, secretAccessKey: String): UIO[AwsCredentialsProvider] =
    const(AwsBasicCredentials.create(accessKeyId, secretAccessKey))

  /**
   * Gives a provider with fixed session credentials, from an access key ID, a secret
   * access key and a session token.
   */
  def session(accessKeyId: String, secretAccessKey: String, sessionToken: String): UIO[AwsCredentialsProvider] =
    const(AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken))

  /**
   * Reads the credentials from the `aws.accessKeyId` and `aws.secretAccessKey` system
   * properties. The effect fails with `InvalidCredentials` when they cannot be
   * resolved.
   */
  val system: IO[InvalidCredentials, SystemPropertyCredentialsProvider] =
    ZIO
      .succeed(SystemPropertyCredentialsProvider.create())
      .tap(c => ZIO.attemptBlocking(c.resolveCredentials()))
      .mapError(err => InvalidCredentials(err.getMessage))

  /**
   * Reads the credentials from the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
   * environment variables. The effect fails with `InvalidCredentials` when they cannot
   * be resolved.
   */
  val env: IO[InvalidCredentials, EnvironmentVariableCredentialsProvider] =
    ZIO
      .succeed(EnvironmentVariableCredentialsProvider.create())
      .tap(c =>
        ZIO
          .attemptBlocking(c.resolveCredentials())
          .mapError(err => InvalidCredentials(err.getMessage))
      )

  /**
   * Reads the credentials of the default profile from the AWS credentials and config
   * files. See `profile(name)`.
   */
  val profile: ZIO[Scope, InvalidCredentials, ProfileCredentialsProvider] =
    profile(None)

  /**
   * Reads the credentials of a profile from the AWS credentials and config files. When
   * `name` is `None`, the profile is the one named by the `aws.profile` system property,
   * or else by the `AWS_PROFILE` environment variable, or `default` when neither is set.
   * The effect fails with `InvalidCredentials` when the profile cannot provide credentials.
   *
   * @param name name of the profile, or `None` to choose it as described above
   */
  def profile(name: Option[String]): ZIO[Scope, InvalidCredentials, ProfileCredentialsProvider] =
    ZIO
      .fromAutoCloseable(ZIO.succeed(ProfileCredentialsProvider.create(name.orNull)))
      .tap(c =>
        ZIO
          .attemptBlocking(c.resolveCredentials())
          .mapError(err => InvalidCredentials(err.getMessage))
      )

  /**
   * Reads the credentials from the container credentials endpoint of ECS or EKS. The
   * effect fails with `InvalidCredentials` when they cannot be resolved.
   */
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

  /**
   * Reads the credentials from the EC2 instance metadata service. The effect fails
   * with `InvalidCredentials` when they cannot be resolved.
   */
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
   * Reads web identity credentials, as used by IAM roles for service accounts. The effect
   * fails with `InvalidCredentials` when they cannot be resolved. zio-s3 depends on the
   * AWS STS module, which the provider needs to exchange the web identity token for
   * credentials.
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
   * Uses the AWS default credentials chain. The chain is resolved when an S3 request
   * needs credentials, not when this effect runs.
   */
  val default: ZIO[Scope, InvalidCredentials, AwsCredentialsProvider] =
    ZIO.fromAutoCloseable(
      ZIO.succeed(DefaultCredentialsProvider.create())
    )
}

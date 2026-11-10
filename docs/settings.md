---
id: settings
title: "Settings and Layers"
sidebar_label: "Settings and Layers"
---

The `S3` service is built as a ZIO layer. ZIO S3 provides several functions that build it, and a settings type, `S3Settings`, that holds a region and credentials. This page lists each function and its parameters. The [configure credentials how-to](./credentials.md) shows how to choose between them.

## Layer Functions

The table lists each function that builds an `S3` layer, and the errors that building it can produce:

| Function                                                          | Result                                    | Builds the client from                          |
| ----------------------------------------------------------------- | ----------------------------------------- | ----------------------------------------------- |
| `live(region, credentials, uriEndpoint?, forcePathStyle?)`        | `Layer[S3Exception, S3]`                  | A fixed `AwsCredentials` value                  |
| `liveZIO(region, provider, uriEndpoint?, forcePathStyle?)`        | `ZLayer[R, S3Exception, S3]`              | An `AwsCredentialsProvider` effect              |
| `live`                                                            | `ZLayer[S3Settings, ConnectionError, S3]` | An `S3Settings` value in the environment        |
| `settings(region, cred)`                                          | `ZLayer[R, S3Exception, S3Settings]`      | An `AwsCredentials` effect, as `S3Settings`     |
| `stub(path)`                                                      | `ZLayer[Any, Nothing, S3]`                | A local directory, for tests                    |

The optional parameters, `uriEndpoint` and `forcePathStyle`, default to `None`. With `None`, the AWS SDK uses its standard endpoint and addressing style. Set `uriEndpoint` to a `java.net.URI` to connect to another server, and set `forcePathStyle` to `Some(true)` to address buckets in the path, as [S3-compatible endpoints](./s3-compatible-endpoints.md) describes.

The `live` function that takes a region and credentials rejects any region that the AWS SDK does not know, and the layer fails with `InvalidSettings`. The `live` value that takes `S3Settings` does not accept an endpoint.

## S3Settings

`S3Settings` is a case class that holds the region and the credentials that the `live` value uses:

```scala
final case class S3Settings(s3Region: S3Region, credentials: AwsCredentials)

object S3Settings {
  def from(region: Region, credentials: AwsCredentials): IO[InvalidSettings, S3Settings]
}
```

`S3Settings.from` checks the region with `S3Region.from`, and fails with `InvalidSettings` when the region is not known to the AWS SDK.

## S3Region

`S3Region` wraps a `Region` that ZIO S3 has checked:

```scala
sealed abstract class S3Region(val region: Region)

object S3Region {
  def from(region: Region): Either[InvalidSettings, S3Region]
  def unsafeFromString(r: String): S3Region
}
```

`S3Region.from` accepts only the regions listed by `Region.regions()`. `S3Region.unsafeFromString` skips that check, so an unknown name is accepted and only fails later, when the client makes a request. Use it only with a server that accepts that name, and only through the `live` value that takes `S3Settings`.

## Credential Providers

The `zio.s3.providers` object contains the providers that `liveZIO` and `profile` accept. The table lists each provider, its type, and whether it needs a `Scope`:

| Provider                 | Type                                                                    | Needs `Scope` |
| ------------------------ | ----------------------------------------------------------------------- | ------------- |
| `const(credentials)`     | `UIO[AwsCredentialsProvider]`                                           | No            |
| `basic(key, secret)`     | `UIO[AwsCredentialsProvider]`                                           | No            |
| `session(key, secret, token)` | `UIO[AwsCredentialsProvider]`                                      | No            |
| `system`                 | `IO[InvalidCredentials, SystemPropertyCredentialsProvider]`             | No            |
| `env`                    | `IO[InvalidCredentials, EnvironmentVariableCredentialsProvider]`        | No            |
| `profile(name?)`         | `ZIO[Scope, InvalidCredentials, ProfileCredentialsProvider]`            | Yes           |
| `container`              | `ZIO[Scope, InvalidCredentials, ContainerCredentialsProvider]`          | Yes           |
| `instanceProfile`        | `ZIO[Scope, InvalidCredentials, InstanceProfileCredentialsProvider]`    | Yes           |
| `webIdentity`            | `ZIO[Scope, InvalidCredentials, WebIdentityTokenFileCredentialsProvider]` | Yes         |
| `default`                | `ZIO[Scope, InvalidCredentials, AwsCredentialsProvider]`                | Yes           |

The providers that check the credentials when they are built fail with `InvalidCredentials`. The `webIdentity` provider needs the `sts` module of the AWS SDK on the classpath, which ZIO S3 already depends on.

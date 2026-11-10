---
id: errors
title: "Errors"
sidebar_label: "Errors"
---

Most ZIO S3 operations fail with `S3Exception`, the error type of the AWS S3 Java client. ZIO S3 adds its own subtypes of `S3Exception` for the failures that it detects itself, and it passes through the subtypes that the AWS SDK raises for S3 responses, such as `NoSuchKeyException`. This page lists each type and when the library raises it.

## Error Types

Each type below extends `S3Exception`, so a handler for `S3Exception` catches all of them:

- `InvalidCredentials(message: String)` is raised by the providers in `zio.s3.providers` when the AWS SDK cannot resolve credentials from that source.
- `InvalidSettings(message: String)` is raised when `S3Region.from` or `S3Settings.from` receives a region that the AWS SDK does not know.
- `ConnectionError(message: String, cause: Throwable)` is raised when the client cannot be built. Building the client wraps a failed credentials provider in this error, so a layer built from a provider fails with `ConnectionError`.
- `DecodingException(cause: CharacterCodingException)` is raised by `streamLines` when the object is not valid UTF-8.
- `SdkError(error: SdkException)` is raised by `execute` when the AWS SDK fails without an S3 response, such as a network error.
- `InvalidPartSize(message: String, size: Int)` is defined in the library, but no code path raises it today. An invalid part size or parallelism stops the effect with a defect instead, as the [multipart upload how-to](./multipart-upload.md) describes.

The AWS SDK raises further subtypes of `S3Exception` for S3 responses, such as `NoSuchKeyException` when an object does not exist and `NoSuchBucketException` when a bucket does not exist. The library passes these through unchanged.

## Handle a Missing Object

To handle a missing object, match the AWS SDK's `NoSuchKeyException` in `catchSome`. Any other `S3Exception` continues to fail:

```scala mdoc:compile-only
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import zio._
import zio.s3._
import zio.stream.ZPipeline

val readOrEmpty: ZIO[S3, Throwable, String] =
  getObject("docs", "missing.txt")
    .via(ZPipeline.utf8Decode)
    .runCollect
    .map(_.mkString)
    .catchSome { case _: NoSuchKeyException => ZIO.succeed("") }
```

## Handle Credential and Connection Failures

To handle a failure while building the layer, match `ConnectionError`. The error message comes from the AWS SDK, so log it or show it to the user rather than matching its text:

```scala mdoc:compile-only
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.s3._
import zio.s3.errors.ConnectionError

import java.net.URI

val connected: ZIO[Any, S3Exception, Unit] =
  listBuckets
    .provideLayer(
      live(
        Region.US_EAST_1,
        AwsBasicCredentials.create("TESTKEY", "TESTSECRET"),
        Some(URI.create("http://localhost:9000")),
        forcePathStyle = Some(true)
      )
    )
    .unit
    .catchSome { case e: ConnectionError => Console.printLine(s"cannot connect: ${e.getMessage}").orDie }
```

The `ConnectionError` type appears in the signature of the `live` value that takes `S3Settings`, and it appears in the error of the layer for a failed provider. Use `catchSome` on the layer, as [configure credentials](./credentials.md#handle-credential-failures) shows, to recover from it.

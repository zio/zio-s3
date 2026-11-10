---
id: s3-compatible-endpoints
title: "Connect to an S3-Compatible Server"
sidebar_label: "S3-Compatible Endpoints"
---

ZIO S3 works with any server that speaks the S3 API, such as MinIO for local development, or a hosted provider that is not AWS. To connect to such a server, pass its address as the endpoint when you build the layer. The endpoint overrides the AWS address that the client would otherwise use.

## Connect to MinIO

To connect to a local MinIO server, pass the server address as `uriEndpoint` and set `forcePathStyle` to `Some(true)`. MinIO expects the bucket name in the path, not in the host name:

```scala mdoc:compile-only
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.s3._

import java.net.URI

val minio: Layer[S3Exception, S3] =
  live(
    Region.US_EAST_1,
    AwsBasicCredentials.create("TESTKEY", "TESTSECRET"),
    Some(URI.create("http://localhost:9000")),
    forcePathStyle = Some(true)
  )
```

The repository's `docker-compose.yml` starts a MinIO server on port 9000 with the credentials `TESTKEY` and `TESTSECRET`, and it creates a bucket named `bucket-1`. Any region works with MinIO, so `Region.US_EAST_1` is a valid choice.

## Connect to Another Provider

To connect to a hosted S3-compatible service, pass its HTTPS address as `uriEndpoint`. Use a region that the provider documents for signing requests, and set `forcePathStyle` only if the provider requires path-style addressing:

```scala mdoc:compile-only
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.s3._

import java.net.URI

val provider: Layer[S3Exception, S3] =
  live(
    Region.EU_CENTRAL_1,
    AwsBasicCredentials.create("access-key", "secret-key"),
    Some(URI.create("https://s3.provider.example.com")),
    forcePathStyle = None
  )
```

The address `s3.provider.example.com` is a placeholder. Use the endpoint from your provider's documentation.

## Know the Region Limits

The `live` and `liveZIO` functions accept only regions from the AWS SDK's list, such as `Region.US_EAST_1`. A region name that the SDK does not know makes the layer fail with `InvalidSettings`, so pick the closest SDK region for the provider's signing name. The `live` layer built from `S3Settings` has no endpoint parameter, so it cannot connect to a custom endpoint. Use `live` or `liveZIO` with an endpoint instead.

## Verify the Connection

To check that the endpoint answers, list the buckets with the layer you built. A wrong address or credentials fail the effect with `ConnectionError` or `S3Exception`, as described in the [errors reference](./errors.md):

```scala mdoc:compile-only
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.s3._

import java.net.URI

val check: ZIO[Any, S3Exception, Unit] =
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
```

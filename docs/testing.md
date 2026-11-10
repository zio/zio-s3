---
id: testing
title: "Test with the Stub"
sidebar_label: "Test with the Stub"
---

ZIO S3 includes a stub implementation of the `S3` service for tests. The stub stores each bucket as a directory under a root path on the local disk, so tests run without a network connection or an S3 server. To build the stub layer, call `stub` with the root path:

```scala mdoc:compile-only
import zio._
import zio.nio.file.{ Path => ZPath }
import zio.s3._

val stubS3: ZLayer[Any, Nothing, S3] = stub(ZPath("/tmp/s3-data"))
```

The root directory must exist before the stub creates a bucket. Each bucket is a subdirectory, and each object is a file inside it, so the layout matches what you would see on disk after a sequence of `createBucket` and `putObject` calls.

## Run Effects Against the Stub

To use the stub in a test, provide the layer to the effect under test with `provideLayer`. The stub keeps its buckets on disk under the root path, so give each test suite its own root directory, and delete it when the suite ends, to keep the buckets of one suite out of another:

```scala mdoc:compile-only
import zio._
import zio.nio.file.{ Path => ZPath }
import zio.s3._

val bucketsAfterCreate: ZIO[Any, Throwable, Boolean] =
  (for {
    _       <- createBucket("tests-bucket")
    buckets <- listBuckets
  } yield buckets.map(_.name).contains("tests-bucket"))
    .provideLayer(stub(ZPath("/tmp/s3-data")))
```

The check for the bucket name runs inside the same effect as the create call, so it reads the bucket that the create call made. Pass the result to the assertion library that your test framework provides.

## Know Where the Stub Differs from AWS

The stub follows the `S3` API for common operations, but a few behaviors differ from a real server, and tests that depend on them can pass against the stub and fail against S3:

- Errors: `createBucket` on an existing name, `deleteBucket` on a missing or non-empty bucket, and `listObjects` on a missing bucket stop with a defect, not a typed `S3Exception`, so `catchSome` on `S3Exception` does not catch them. Test those cases against a real server.
- Metadata: the content type and user metadata that `putObject` stores live in memory. They are lost when the JVM exits, and files that were written outside `putObject`, such as the files already in the root directory, report an empty content type and no metadata.
- Pagination: the stub does not return correct pages when a listing has a `maxKeys` limit and more objects than that limit. Do not test pagination against the stub. Use a MinIO server as shown in [list and paginate objects](./list-and-paginate-objects.md).
- Raw client: `execute` is not supported and stops with a defect, so code that calls `execute` needs a live layer in tests.

For tests that need the real behavior, run a MinIO server as shown in the [introduction](./index.md) and build a `live` layer with `forcePathStyle = Some(true)`.

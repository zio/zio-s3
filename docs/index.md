---
id: index
title: "Introduction to ZIO S3"
sidebar_label: "ZIO S3"
---

[ZIO S3](https://github.com/zio/zio-s3) is a thin wrapper over the AWS S3 async client for ZIO.

@PROJECT_BADGES@

## Introduction

ZIO S3 exposes the main operations of the AWS S3 Java client as ZIO effects and ZIO Streams. Each operation needs an `S3` service, which we provide with a layer built from a region and credentials:

```scala mdoc:compile-only
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.s3._
import zio.stream.ZStream

val s3Layer: Layer[S3Exception, S3] =
  live(Region.US_EAST_1, AwsBasicCredentials.create("accessKeyId", "secretAccessKey"))

// list all buckets available
val buckets: ZIO[S3, S3Exception, Unit] =
  listBuckets.unit

// list all objects of all buckets
val allObjects: ZStream[S3, S3Exception, String] =
  ZStream
    .fromIterableZIO(listBuckets)
    .flatMap(bucket => listAllObjects(bucket.name).map(obj => obj.bucketName + "/" + obj.key))
```

All operations live in the package object `zio.s3`, so a single `import zio.s3._` brings them into scope. The rest of this site covers each area in depth, starting with the [tutorial](./tutorial.md).

## Installation

Add the following line to your `build.sbt` file:

```scala
libraryDependencies += "dev.zio" %% "zio-s3" % "@VERSION@"
```

## Example 1

To run the following example locally, start an instance of _MinIO_, an object storage server compatible with the S3 API:

```bash
docker run -p 9000:9000 -e MINIO_ROOT_USER=MyKey -e MINIO_ROOT_PASSWORD=MySecret minio/minio server --compat /data
```

This example creates a bucket, stores a JSON object in it, and reads the object back:

```scala mdoc:compile-only
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import zio._
import zio.s3._
import zio.stream.{ ZPipeline, ZStream }

import java.net.URI

object ZIOS3Example extends ZIOAppDefault {

  val json: Chunk[Byte] = Chunk.fromArray("""{  "id" : 1 , "name" : "A1" }""".getBytes)

  val myApp: ZIO[S3, Throwable, Unit] = for {
    _ <- createBucket("docs")
    _ <- putObject(
           bucketName = "docs",
           key = "doc1",
           contentLength = json.length.toLong,
           content = ZStream.fromChunk(json),
           options = UploadOptions.fromContentType("application/json")
         )
    _ <- getObject("docs", "doc1")
           .via(ZPipeline.utf8Decode)
           .foreach(Console.printLine(_))
  } yield ()

  def run =
    myApp.provide(
      live(
        Region.CA_CENTRAL_1,
        AwsBasicCredentials.create("MyKey", "MySecret"),
        Some(URI.create("http://localhost:9000")),
        forcePathStyle = Some(true) // MinIO requires path-style requests
      )
    )
}
```

## Example 2

This example uploads a small object, streams a large file with multipart upload, and downloads an object to a local file:

```scala mdoc:compile-only
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.stream.{ ZSink, ZStream }
import zio.s3._

import java.io.FileOutputStream
import java.nio.file.Paths

// upload a small object
val json: Chunk[Byte] = Chunk.fromArray("""{  "id" : 1 , "name" : "A1" }""".getBytes)
val up: ZIO[S3, S3Exception, Unit] = putObject(
  "bucket-1",
  "user.json",
  json.length.toLong,
  ZStream.fromChunk(json),
  UploadOptions.fromContentType("application/json")
)

// upload a large file in parts, with 4 parts uploaded in parallel
val large: ZIO[S3, S3Exception, Unit] = multipartUpload(
  "bucket-1",
  "upload/myfile.zip",
  ZStream.fromPath(Paths.get("/my/path/to/myfile.zip")),
  MultipartUploadOptions.fromUploadOptions(UploadOptions.fromContentType("application/zip"))
)(4)

// download an object to a local file
val download =
  ZIO.scoped {
    ZIO
      .fromAutoCloseable(ZIO.attempt(new FileOutputStream("/my/path/to/download.zip")))
      .flatMap(os => getObject("bucket-1", "upload/myfile.zip").run(ZSink.fromOutputStream(os)))
  }
```

## Accessing the Underlying Client

If the library does not wrap an operation you need, call `execute` to run it on the underlying `S3AsyncClient`. It takes a function from the client to a `CompletableFuture` and returns the result as a ZIO effect that fails with `S3Exception`:

```scala mdoc:compile-only
import software.amazon.awssdk.services.s3.model.{ HeadBucketRequest, S3Exception }
import zio._
import zio.s3._

val bucketExists: ZIO[S3, S3Exception, Boolean] =
  execute(_.headBucket(HeadBucketRequest.builder().bucket("docs").build())).as(true)
```

The [errors reference](./errors.md) lists the error types that `execute` can return.

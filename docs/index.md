---
id: index
title: "Introduction to ZIO S3"
sidebar_label: "ZIO S3"
---

[ZIO S3](https://github.com/zio/zio-s3) is a thin wrapper over S3 async client for ZIO.

@PROJECT_BADGES@

## Introduction

ZIO-S3 is a thin wrapper over the s3 async java client. It exposes the main operations of the s3 java client.

```scala
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import zio.Chunk
import zio.s3._
import zio.stream.{ZSink, ZStream}
import software.amazon.awssdk.services.s3.model.S3Exception

  // list all buckets available  
  listBuckets.provideLayer(
     live("us-east-1", AwsBasicCredentials.create("accessKeyId", "secretAccessKey"))
  )
  
  // list all objects of all buckets
  val l2: ZStream[S3, S3Exception, String] = (for {
     bucket <- ZStream.fromIterableZIO(listBuckets) 
     obj <- listAllObjects(bucket.name)
  } yield obj.bucketName + "/" + obj.key).provideLayer(
     live("us-east-1", AwsBasicCredentials.create("accessKeyId", "secretAccessKey"))
  )  
```

All available s3 combinators and operations are available in the package object `zio.s3`, you only need to `import zio.s3._`

## Installation

In order to use this library, we need to add the following line in our `build.sbt` file:

```scala
libraryDependencies += "dev.zio" %% "zio-s3" % "@VERSION@"
```

## Example 1

Let's try an example of creating a bucket and adding an object into it. To run this example, we need to run an instance of _Minio_ which is object storage compatible with S3:

```bash
docker run -p 9000:9000 -e MINIO_ROOT_USER=MyKey -e MINIO_ROOT_PASSWORD=MySecret minio/minio  server --compat /data
```

In this example we create a bucket and then add a JSON object to it and then retrieve that:

```scala mdoc:compile-only
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import zio._
import zio.s3._
import zio.stream.{ZStream, ZPipeline}
import zio.Chunk

import java.net.URI

object ZIOS3Example extends ZIOAppDefault {

  val myApp = for {
    _ <- createBucket("docs")
    json = Chunk.fromArray("""{  "id" : 1 , "name" : "A1" }""".getBytes)
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
    myApp
      .provide(
        live(
          Region.CA_CENTRAL_1,
          AwsBasicCredentials.create("MyKey", "MySecret"),
          Some(URI.create("http://localhost:9000")),
          forcePathStyle = true // Required for path-style S3 requests (MinIO by default uses them)
        )
      )
}
```

## Example 2

```scala mdoc:compile-only
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.stream.{ ZSink, ZStream }
import zio.s3._

// upload
val json: Chunk[Byte] = Chunk.fromArray("""{  "id" : 1 , "name" : "A1" }""".getBytes)
val up: ZIO[S3, S3Exception, Unit] = putObject(
  "bucket-1",
  "user.json",
  json.length.toLong,
  ZStream.fromChunk(json),
  UploadOptions.fromContentType("application/json")
)

// multipartUpload 
import java.io.FileInputStream
import java.nio.file.Paths

val is = ZStream.fromInputStream(new FileInputStream(Paths.get("/my/path/to/myfile.zip").toFile))
val proc2: ZIO[S3, S3Exception, Unit] =
  multipartUpload(
    "bucket-1",
    "upload/myfile.zip",
    is,
    MultipartUploadOptions.fromUploadOptions(UploadOptions.fromContentType("application/zip"))
  )(4)

// download
import java.io.OutputStream

val os: OutputStream = ???
val proc3: ZIO[S3, Exception, Long] = getObject("bucket-1", "upload/myfile.zip").run(ZSink.fromOutputStream(os))
```

## Support any commands?

If you need a method which is not wrapped by the library, you can have access to underlying S3 client in a safe manner by using

```scala
import java.util.concurrent.CompletableFuture
import zio.s3._
import software.amazon.awssdk.services.s3.S3AsyncClient
 
def execute[T](f: S3AsyncClient => CompletableFuture[T]) 
```

---
id: quickstart_index
title: "Quick Start"
---

Setup
-----

```
//support scala 2.12 / 2.13

libraryDependencies += "dev.zio" %% "zio-s3" % "<version>"
```

How to use it ?
---------------

ZIO-S3 is a thin wrapper over the s3 async java client. It exposes the main operations of the s3 java client.


```scala
import zio.{Chunk, ZManaged}
import zio.s3._
import zio.stream.{ZSink, ZStream}
import software.amazon.awssdk.services.s3.model.S3Exception

  // list all buckets available  
  listBuckets.provideLayer(
     live("us-east-1", S3Credentials("accessKeyId", "secretAccessKey"))
  )
  
  // list all objects of all buckets
  val l2: ZStream[S3, S3Exception, String] = (for {
     bucket <- ZStream.fromIterableM(listBuckets) 
     obj <- listObjectsDescendant(bucket.name, "")
  } yield obj.bucketName + "/" + obj.key).provideLayer(
     live("us-east-1", S3Credentials("accessKeyId", "secretAccessKey"))
  )  
```

All available s3 combinators and operations are available in the package object `zio.s3`, you only need to `import zio.s3._`


Test / Stub
-----------

a stub implementation of s3 storage is provided for testing purpose and use internally a filesystem to simulate s3 storage

```scala
import zio.nio.core.file.{Path => ZPath}
import zio.s3._
import zio.blocking.Blocking

// required to provide a Blocking context
val testS3: ZLayer[Blocking, Any, S3] = test(ZPath("/tmp/s3-data")) 

// use a Blocking context to build s3 Layer
val s3: ZLayer[Any, Any, S3] = Blocking.live >>> test(ZPath("/tmp/s3-data"))

// list all buckets available by using S3 Stub Layer 
// will list all directories of `/tmp/s3-data`
listBuckets.provideLayer(s3) 
```

More informations here how to use [ZLayer https://zio.dev/docs/howto/howto_use_layers](https://zio.dev/docs/howto/howto_use_layers)


Examples
--------

```scala
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.blocking.Blocking
import zio.stream.{ ZSink, ZStream }
import zio.s3._

// upload
val json: Chunk[Byte] = Chunk.fromArray("""{  "id" : 1 , "name" : "A1" }""".getBytes)
val up: ZIO[S3, S3Exception, Unit] = putObject(
  "bucket-1",
  "user.json",
  json.length,
  ZStream.fromChunk(json),
  UploadOptions(contentType = Some("application/json"))
)

// multipartUpload 
import java.io.FileInputStream
import java.nio.file.Paths

val is = ZStream.fromInputStream(new FileInputStream(Paths.get("/my/path/to/myfile.zip").toFile))
val proc2: ZIO[S3 with Blocking, S3Exception, Unit] =
  multipartUpload(
    "bucket-1",
    "upload/myfile.zip",
    is,
    MultipartUploadOptions(UploadOptions(contentType = Some("application/zip")))
  )(4)

// download
import java.io.OutputStream

val os: OutputStream = ???
val proc3: ZIO[Blocking with S3, Exception, Long] = getObject("bucket-1", "upload/myfile.zip").run(ZSink.fromOutputStream(os))
```

Support any commands ?
---

If you need a method which is not wrapped by the library, you can have access to underlying S3 client in a safe manner by using

```scala
import java.util.concurrent.CompletableFuture
import zio.s3._
import software.amazon.awssdk.services.s3.S3AsyncClient
 
def execute[T](f: S3AsyncClient => CompletableFuture[T]) 
```

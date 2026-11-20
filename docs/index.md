---
id: index
title: "Introduction to ZIO S3"
sidebar_label: "Introduction"
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

All operations live in the package object `zio.s3`, so a single `import zio.s3._` brings them into scope. To run a first program end to end, follow [Getting Started](./getting-started.md). For a guided walk-through, see the [tutorial](./tutorial.md).

## Installation

Add ZIO S3 to your `build.sbt`. The library is published for the JVM only, for Scala 2.12, 2.13 and 3:

```sbt
// ZIO S3 client for the JVM, built on the AWS SDK v2 async client
libraryDependencies += "dev.zio" %% "zio-s3" % "@VERSION@"
```

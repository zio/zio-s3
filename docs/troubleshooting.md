---
id: troubleshooting
title: "Troubleshooting"
---

This page covers common issues and their solutions when working with ZIO S3.

## Out of Memory with `putObject` on Concurrent Uploads

When uploading many large files concurrently using `putObject`, you may encounter `OutOfMemoryError` due to JVM's `HeapByteBuffer` caching behavior. The JVM caches `HeapByteBuffer` instances per thread, and with many concurrent uploads of large files, this can lead to excessive memory consumption.

See [#247](https://github.com/zio/zio-s3/issues/247) for more details about this issue.

### Workarounds

#### 1. Limit Concurrent Uploads with Semaphore

Control the number of concurrent uploads to prevent excessive buffer allocation:

```scala mdoc:compile-only
import zio._
import zio.s3._
import zio.stream.ZStream

def uploadFiles(files: List[(String, String, ZStream[Any, Throwable, Byte])]): ZIO[S3, Throwable, Unit] =
  for {
    semaphore <- Semaphore.make(50) // limit to 50 concurrent uploads
    _ <- ZIO.foreachPar(files) { case (bucket, key, content) =>
      semaphore.withPermit {
        for {
          bytes <- content.runCollect
          _ <- putObject(bucket, key, bytes.length.toLong, ZStream.fromChunk(bytes))
        } yield ()
      }
    }
  } yield ()
```

#### 2. Set JVM Option to Limit Buffer Cache Size

Add the following JVM option to limit the maximum size of cached buffers:

```
-Djdk.nio.maxCachedBufferSize=1048576
```

This limits the cached buffer size to 1MB, preventing excessive memory usage from large buffer caches.

#### 3. Use `multipartUpload` for Large Files

For files larger than 5MB, prefer `multipartUpload` which handles large files more efficiently:

```scala mdoc:compile-only
import zio._
import zio.s3._
import zio.stream.ZStream
import software.amazon.awssdk.services.s3.model.S3Exception
import java.io.FileInputStream
import java.nio.file.Path

def uploadLargeFile(bucket: String, key: String, path: Path): ZIO[S3, S3Exception, Unit] = {
  val stream = ZStream.fromInputStream(new FileInputStream(path.toFile))
  multipartUpload(bucket, key, stream)(parallelism = 10)
}
```

### Choosing the Right Approach

| Scenario | Recommended Approach |
|----------|---------------------|
| Many small files (< 5MB) | Semaphore + `putObject` |
| Large files (> 5MB) | `multipartUpload` |
| Mixed workload | Combine Semaphore with size-based routing |
| Memory-constrained environment | JVM option + Semaphore |

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
import software.amazon.awssdk.services.s3.model.S3Exception

// For files >= 5MB, use multipartUpload with semaphore to limit concurrency
def uploadFiles(files: List[(String, String, ZStream[Any, Throwable, Byte])]): ZIO[S3, S3Exception, Unit] =
  for {
    semaphore <- Semaphore.make(50) // limit to 50 concurrent uploads
    _ <- ZIO.foreachPar(files) { case (bucket, key, content) =>
      semaphore.withPermit {
        multipartUpload(bucket, key, content)(parallelism = 1)
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
| Small files (< 5MB) | `putObject` with known content length |
| Large files (≥ 5MB) | Semaphore + `multipartUpload` |
| Mixed workload | Size-based routing to appropriate method |
| Memory-constrained environment | JVM option + reduced concurrency |

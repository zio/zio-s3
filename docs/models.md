---
id: models
title: "Model Types"
sidebar_label: "Model Types"
---

The operations return values of a few model types. Each type is a case class that holds the fields of an S3 response, converted to Scala types. The [operations reference](./s3-operations.md) lists the operation that returns each type.

## S3Bucket

`S3Bucket` describes one bucket returned by `listBuckets`:

```scala
final case class S3Bucket(name: String, creationDate: Instant)

object S3Bucket {
  type S3BucketListing = Chunk[S3Bucket]
}
```

The `name` field is the bucket name, and `creationDate` is the time the bucket was created. The alias `S3BucketListing` is the type that `listBuckets` returns, a `Chunk` of buckets.

## S3ObjectListing

`S3ObjectListing` describes one page of a listing from `listObjects` or `getNextObjects`:

```scala
final case class S3ObjectListing(
  bucketName: String,
  delimiter: Option[String],
  starAfter: Option[String],
  objectSummaries: Chunk[S3ObjectSummary],
  nextContinuationToken: Option[String],
  prefix: Option[String]
)
```

The `objectSummaries` field holds the objects of the page. A `nextContinuationToken` of `None` means the page is the last one. The [list and paginate objects how-to](./list-and-paginate-objects.md) shows how to use the token.

## S3ObjectSummary

`S3ObjectSummary` describes one object in a listing:

```scala
final case class S3ObjectSummary(bucketName: String, key: String, lastModified: Instant, size: Long)
```

The `key` field is the full key of the object, including any slashes, and `size` is the object's length in bytes.

## ObjectMetadata

`ObjectMetadata` describes one object as returned by `getObjectMetadata`:

```scala
final case class ObjectMetadata(
  metadata: Map[String, String],
  contentType: String,
  contentLength: Long,
  eTag: String
)
```

The `metadata` field holds the user-defined metadata without the `x-amz-meta-` prefix, and `eTag` holds the ETag without the surrounding quotes. The [object metadata how-to](./object-metadata.md) shows how to read these fields.

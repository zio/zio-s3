/*
 * Copyright 2017-2020 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.s3

import software.amazon.awssdk.services.s3.model.{ Bucket, HeadObjectResponse, ListObjectsV2Response }
import zio.Chunk

import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * A bucket of the storage, with its `name` and its `creationDate`.
 */
final case class S3Bucket(name: String, creationDate: Instant)

/**
 * Constructors of `S3Bucket` and the `S3BucketListing` type.
 */
object S3Bucket {

  /**
   * The result of `listBuckets`: the buckets as a `Chunk`.
   */
  type S3BucketListing = Chunk[S3Bucket]

  /**
   * Converts an AWS `Bucket` into an `S3Bucket` with its name and creation date.
   */
  def fromBucket(bucket: Bucket): S3Bucket =
    new S3Bucket(bucket.name(), bucket.creationDate())

  /**
   * Converts a list of AWS buckets into an `S3BucketListing`, keeping the order.
   */
  def fromBuckets(l: List[Bucket]): S3BucketListing =
    Chunk.fromIterable(l.map(fromBucket))
}

/**
 * One page of a listing of the objects in a bucket. `nextContinuationToken` is set
 * when more pages exist. `prefix` is `None` when the page has no prefix.
 */
final case class S3ObjectListing(
  bucketName: String,
  delimiter: Option[String],
  starAfter: Option[String],
  objectSummaries: Chunk[S3ObjectSummary],
  nextContinuationToken: Option[String],
  prefix: Option[String]
)

/**
 * Constructors of `S3ObjectListing`.
 */
object S3ObjectListing {

  /**
   * Builds an empty page for `bucketName`, with the continuation token
   * `nextContinuationToken`.
   *
   * @param bucketName name of the bucket
   * @param nextContinuationToken token of the next page, or `None`
   */
  def from(bucketName: String, nextContinuationToken: Option[String]): S3ObjectListing =
    S3ObjectListing(bucketName, None, None, Chunk.empty, nextContinuationToken, None)

  /**
   * Builds a page from an AWS `ListObjectsV2Response`. The object summaries come from
   * the contents of the response. `prefix` is `None` when the response has an empty
   * prefix.
   */
  def fromResponse(r: ListObjectsV2Response): S3ObjectListing =
    S3ObjectListing(
      r.name(),
      Option(r.delimiter()),
      Option(r.startAfter()),
      Chunk
        .fromIterable(r.contents().asScala.toList)
        .map(o => S3ObjectSummary(r.name(), o.key(), o.lastModified(), o.size())),
      Option(r.nextContinuationToken()),
      Option(r.prefix()).collect { case x if x.nonEmpty => x }
    )

}

/**
 * The summary of an object: the name of its bucket, its `key`, its last modification
 * time and its size in bytes.
 */
final case class S3ObjectSummary(bucketName: String, key: String, lastModified: Instant, size: Long)

/**
 * The metadata of an object, from a HEAD request.
 *
 * @param metadata the user-defined metadata without the "x-amz-meta-" prefix
 * @param contentType the content type of the object (application/json, application/zip, text/plain, ...)
 * @param contentLength the size of the object in bytes
 * @param eTag the ETag of the response, without the double quotes. For a multipart upload it is not the MD5 of the content: it is a hex string followed by `-` and the number of parts
 */
final case class ObjectMetadata(
  metadata: Map[String, String],
  contentType: String,
  contentLength: Long,
  eTag: String
)

/**
 * Constructors of `ObjectMetadata`.
 */
object ObjectMetadata {

  /**
   * Builds the metadata from an AWS `HeadObjectResponse`, removing the double quotes
   * from the ETag.
   */
  def fromResponse(r: HeadObjectResponse): ObjectMetadata =
    ObjectMetadata(
      r.metadata().asScala.toMap,
      r.contentType(),
      r.contentLength(),
      // ETag is including quotes
      r.eTag().replace("\"", "")
    )
}

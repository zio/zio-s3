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

import java.time.Instant

import software.amazon.awssdk.services.s3.model.{ Bucket, HeadObjectResponse, ListObjectsV2Response }
import zio.Chunk

import scala.jdk.CollectionConverters._

final case class S3Bucket(name: String, creationDate: Instant)

object S3Bucket {
  type S3BucketListing = Chunk[S3Bucket]

  def fromBucket(bucket: Bucket): S3Bucket =
    new S3Bucket(bucket.name(), bucket.creationDate())

  def fromBuckets(l: List[Bucket]): S3BucketListing =
    Chunk.fromIterable(l.map(fromBucket))
}

final case class S3ObjectListing(
  bucketName: String,
  delimiter: Option[String],
  starAfter: Option[String],
  objectSummaries: Chunk[S3ObjectSummary],
  nextContinuationToken: Option[String],
  prefix: Option[String]
)

object S3ObjectListing {

  def from(bucketName: String, nextContinuationToken: Option[String]) =
    S3ObjectListing(bucketName, None, None, Chunk.empty, nextContinuationToken, None)

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

final case class S3ObjectSummary(bucketName: String, key: String, lastModified: Instant, size: Long)

/**
 * @param metadata the user-defined metadata without the "x-amz-meta-" prefix
 * @param contentType the content type of the object (application/json, application/zip, text/plain, ...)
 * @param contentLength the size of the object in bytes
 */
final case class ObjectMetadata(metadata: Map[String, String], contentType: String, contentLength: Long)

object ObjectMetadata {

  def fromResponse(r: HeadObjectResponse): ObjectMetadata =
    ObjectMetadata(r.metadata().asScala.toMap, r.contentType(), r.contentLength())
}

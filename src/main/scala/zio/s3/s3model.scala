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

import software.amazon.awssdk.services.s3.model.{ Bucket, ListObjectsV2Response }
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
  objectSummaries: Chunk[S3ObjectSummary],
  nextContinuationToken: Option[String]
)

object S3ObjectListing {

  def fromResponse(r: ListObjectsV2Response): S3ObjectListing =
    S3ObjectListing(
      r.name(),
      Chunk.fromIterable(r.contents().asScala.toList).map(o => S3ObjectSummary(r.name(), o.key())),
      Option(r.nextContinuationToken())
    )
}

final case class S3ObjectSummary(bucketName: String, key: String)

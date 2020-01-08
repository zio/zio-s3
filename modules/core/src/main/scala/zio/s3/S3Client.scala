package zio.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.Bucket
import zio.blocking.Blocking
import zio.{ RIO, ZIO }

final class S3Client(unsafeClient: AmazonS3) {

  /**
    * Creates a bucket with a specified name.
    *
    * @param name desired bucket name
    * @return a [[com.amazonaws.services.s3.model.Bucket]] instance
    */
  def createBucket(name: String): RIO[Blocking, Bucket] =
    ZIO(unsafeClient.createBucket(name)).catchAll(ZIO.fail)

  /**
    * Deletes a bucket with a specified name.
    *
    * @param name bucket name to delete
    */
  def deleteBucket(name: String): RIO[Blocking, Unit] =
    ZIO(unsafeClient.deleteBucket(name)).catchAll(ZIO.fail)

}

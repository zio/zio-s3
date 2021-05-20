package zio.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import zio.test.Assertion._
import zio.test._

object S3SettingsTest extends DefaultRunnableSpec {

  def spec =
    suite("Settings")(
      testM("invalid region") {
        for {
          failure <- S3Settings
                       .from(Region.of("invalid"), AwsBasicCredentials.create("key", "secret"))
                       .foldCause(_.failureOption.map(_.message).mkString, _ => "")
        } yield assert(failure)(equalTo("Invalid aws region provided : invalid"))
      },
      testM("valid region") {
        for {
          success <- S3Settings.from(Region.US_EAST_2, AwsBasicCredentials.create("key", "secret"))
        } yield assert(success.s3Region.region -> success.credentials)(
          equalTo(Region.US_EAST_2             -> AwsBasicCredentials.create("key", "secret"))
        )
      },
      test("unsafe Region") {
        assert(S3Region.unsafeFromString("blah").region)(equalTo(Region.of("blah")))
      }
    )
}

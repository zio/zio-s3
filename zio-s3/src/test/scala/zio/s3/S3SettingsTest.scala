package zio.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import zio.s3.errors._
import zio.test._

object S3SettingsTest extends ZIOSpecDefault {

  def spec: Spec[Any, InvalidSettings] =
    suite("Settings")(
      test("invalid region") {
        for {
          failure <- S3Settings
                       .from(Region.of("invalid"), AwsBasicCredentials.create("key", "secret"))
                       .foldCause(_.failureOption.map(_.message).mkString, _ => "")
        } yield assertTrue(failure == "Invalid aws region provided : invalid")
      },
      test("valid region") {
        for {
          success <- S3Settings.from(Region.US_EAST_2, AwsBasicCredentials.create("key", "secret"))
        } yield assertTrue(
          success.s3Region.region -> success.credentials ==
            Region.US_EAST_2      -> AwsBasicCredentials.create("key", "secret")
        )
      },
      test("unsafe Region") {
        assertTrue(S3Region.unsafeFromString("blah").region == Region.of("blah"))
      }
    )
}

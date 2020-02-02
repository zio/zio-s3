package zio.s3

import software.amazon.awssdk.regions.Region
import zio.test.Assertion._
import zio.test.{ DefaultRunnableSpec, _ }

object S3SettingsTest
    extends DefaultRunnableSpec(
      suite("settings")(
        testM("invalid region") {
          for {
            failure <- S3Settings
                        .from(Region.of("invalid"), S3Credentials("key", "secret"))
                        .foldCause(_.failureOption.map(_.message).mkString, _ => "")
          } yield assert(failure, equalTo("Invalid aws region provided : invalid"))
        },
        testM("valid region") {
          for {
            success <- S3Settings.from(Region.US_EAST_2, S3Credentials("key", "secret"))
          } yield assert(success, equalTo(S3Settings(Region.US_EAST_2, S3Credentials("key", "secret"))))
        }
      )
    )

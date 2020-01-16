package zio.s3

import software.amazon.awssdk.regions.Region
import zio.test.DefaultRunnableSpec
import zio.test.Assertion._
import zio.test._
import java.net.URI

object S3SettingsTest
    extends DefaultRunnableSpec(
      suite("settings")(
        testM("invalid region") {
          for {
            failure <- S3Settings
                        .from(Region.of("invalid"), S3Credentials("key", "secret"), None)
                        .foldCause(_.failureOption.map(_.message).mkString, _ => "")
          } yield assert(failure, equalTo("Invalid aws region provided : invalid"))
        },
        testM("invalid endpoint") {
          for {
            failure <- S3Settings
                        .from(Region.US_EAST_2, S3Credentials("key", "secret"), "{invalid}")
                        .foldCause(_.failureOption.map(_.message).mkString, _ => "")
          } yield assert(failure, equalTo("Invalid uri endpoint : {invalid}"))
        },
        testM("valid region") {
          for {
            success <- S3Settings.from(Region.US_EAST_2, S3Credentials("key", "secret"), None)
          } yield assert(success, equalTo(S3Settings(Region.US_EAST_2, S3Credentials("key", "secret"), None)))
        },
        testM("valid endpoint") {
          for {
            success <- S3Settings.from(Region.US_EAST_2, S3Credentials("key", "secret"), "http://localhost:9090")
          } yield assert(
            success,
            equalTo(
              S3Settings(Region.US_EAST_2, S3Credentials("key", "secret"), Some(URI.create("http://localhost:9090")))
            )
          )
        }
      )
    )

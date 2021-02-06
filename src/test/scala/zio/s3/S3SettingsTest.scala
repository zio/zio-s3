package zio.s3

import software.amazon.awssdk.regions.Region
import zio.UIO
import zio.test.Assertion._
import zio.test._
import zio.test.TestAspect._

object S3SettingsTest extends DefaultRunnableSpec {

  def setProps(props: (String, String)*): UIO[Unit] =
    UIO {
      props.foreach {
        case (k, v) =>
          System.setProperty(k, v)
      }
    }

  def unsetProps(keys: String*): UIO[Unit] =
    UIO {
      keys.foreach(System.clearProperty)
    }

  def spec =
    suite("Settings")(
      suite("Regions")(
        testM("invalid region") {
          for {
            failure <- S3Settings
                         .from(Region.of("invalid"), S3Credentials("key", "secret"))
                         .foldCause(_.failureOption.map(_.message).mkString, _ => "")
          } yield assert(failure)(equalTo("Invalid aws region provided : invalid"))
        },
        testM("valid region") {
          for {
            success <- S3Settings.from(Region.US_EAST_2, S3Credentials("key", "secret"))
          } yield assert(success.s3Region.region -> success.credentials)(
            equalTo(Region.US_EAST_2             -> S3Credentials("key", "secret"))
          )
        }
      ),
      suite("credentials")(
        testM("cred with const") {
          assertM(S3Credentials.const("k", "v"))(equalTo(S3Credentials("k", "v")))
        },
        testM("cred with default fallback const") {
          assertM(S3Credentials.fromEnv <> S3Credentials.const("k", "v"))(equalTo(S3Credentials("k", "v")))
        },
        testM("cred in system properties") {
          for {
            cred <- S3Credentials.fromSystem
          } yield assert(cred)(equalTo(S3Credentials("k1", "s1")))
        } @@ flaky @@ around_(
          setProps(("aws.accessKeyId", "k1"), ("aws.secretAccessKey", "s1")),
          unsetProps("aws.accessKeyId", "aws.secretAccessKey")
        ),
        testM("no cred in system properties") {
          for {
            failure <- S3Credentials.fromSystem.flip.map(_.message)
          } yield assert(failure)(isNonEmptyString)
        } @@ around_(
          unsetProps("aws.accessKeyId", "aws.secretAccessKey"),
          UIO.unit
        ),
        testM("no cred in environment properties") {
          for {
            failure <- S3Credentials.fromEnv.flip.map(_.message)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in profile") {
          for {
            failure <- S3Credentials.fromProfile.flip.map(_.message)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in container") {
          for {
            failure <- S3Credentials.fromContainer.flip.map(_.message)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in instance profile credentials") {
          for {
            failure <- S3Credentials.fromInstanceProfile.flip.map(_.message)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in webidentity credentials") {
          for {
            failure <- S3Credentials.fromWebIdentity.flip.map(_.message)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred when chain all providers") {
          for {
            failure <- S3Credentials.fromAll.flip.map(_.message)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("settings from invalid creds") {
          for {
            failure <- settings(Region.AF_SOUTH_1, S3Credentials.fromSystem).build.useNow.flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        }
      ) @@ sequential
    )

}

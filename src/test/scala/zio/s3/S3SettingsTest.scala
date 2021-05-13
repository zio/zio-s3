package zio.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ UIO, ZIO }

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
      ),
      suite("credentials")(
        testM("cred with const") {
          assertM(CredentialsProviders.const("k", "v").useNow.map(_.resolveCredentials()))(
            equalTo(AwsBasicCredentials.create("k", "v"))
          )
        },
        testM("cred with default fallback const") {
          assertM(
            (CredentialsProviders.env <> CredentialsProviders.const("k", "v")).useNow.map(_.resolveCredentials())
          )(equalTo(AwsBasicCredentials.create("k", "v")))
        },
        testM("cred in system properties") {
          for {
            cred <- CredentialsProviders.system.use(p => ZIO(p.resolveCredentials()))
          } yield assert(cred)(equalTo(AwsBasicCredentials.create("k1", "s1")))
        } @@ flaky @@ around_(
          setProps(("aws.accessKeyId", "k1"), ("aws.secretAccessKey", "s1")),
          unsetProps("aws.accessKeyId", "aws.secretAccessKey")
        ),
        testM("no cred in system properties") {
          for {
            failure <- CredentialsProviders.system.useNow.flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        } @@ around_(
          unsetProps("aws.accessKeyId", "aws.secretAccessKey"),
          UIO.unit
        ),
        testM("no cred in environment properties") {
          for {
            failure <- CredentialsProviders.env.useNow.flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in profile") {
          for {
            failure <- CredentialsProviders.profile.useNow.flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in container") {
          for {
            failure <- CredentialsProviders.container.useNow.flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in instance profile credentials") {
          for {
            failure <- CredentialsProviders.instanceProfile.useNow.flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("no cred in webidentity credentials") {
          for {
            failure <- CredentialsProviders.webIdentity.useNow.flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        },
        testM("settings from invalid creds") {
          for {
            failure <- settings(
                         Region.AF_SOUTH_1,
                         CredentialsProviders.system.useNow.map(_.resolveCredentials())
                       ).build.useNow.flip
          } yield assert(failure.getMessage)(isNonEmptyString)
        },
        testM("no cred when chain all providers") {
          for {
            failure <- CredentialsProviders.default.use(c => ZIO.effect(c.resolveCredentials())).flip.map(_.getMessage)
          } yield assert(failure)(isNonEmptyString)
        }
      ) @@ sequential
    )

}

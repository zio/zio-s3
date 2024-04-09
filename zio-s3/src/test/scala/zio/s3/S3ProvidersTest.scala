package zio.s3

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, AwsSessionCredentials }
import software.amazon.awssdk.regions.Region
import zio.s3.providers._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Scope, UIO, ZIO }

object S3ProvidersTest extends ZIOSpecDefault {

  def setProps(props: (String, String)*): UIO[Unit] =
    ZIO.succeed {
      props.foreach {
        case (k, v) =>
          System.setProperty(k, v)
      }
    }

  def unsetProps(keys: String*): UIO[Unit] =
    ZIO.succeed {
      keys.foreach(System.clearProperty)
    }

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Providers")(
      test("basic credentials") {
        ZIO
          .scoped[Any](basic("k", "v").map(_.resolveCredentials()))
          .map(res => assertTrue(res == AwsBasicCredentials.create("k", "v")))
      },
      test("session credentials") {
        ZIO
          .scoped[Any](session("k", "v", "t").map(_.resolveCredentials()))
          .map(res => assertTrue(res == AwsSessionCredentials.create("k", "v", "t")))
      },
      test("basic credentials default fallback const") {
        ZIO
          .scoped[Any]((env <> basic("k", "v")).map(_.resolveCredentials()))
          .map(res => assertTrue(res == AwsBasicCredentials.create("k", "v")))
      },
      test("cred in system properties") {
        for {
          cred <- ZIO.scoped[Any](system.flatMap(p => ZIO.attempt(p.resolveCredentials())))
        } yield assertTrue(cred == AwsBasicCredentials.create("k1", "s1"))
      } @@ flaky @@ around(
        setProps(("aws.accessKeyId", "k1"), ("aws.secretAccessKey", "s1")),
        unsetProps("aws.accessKeyId", "aws.secretAccessKey")
      ),
      test("no cred in system properties") {
        for {
          failure <- ZIO.scoped[Any](system).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      } @@ around(
        unsetProps("aws.accessKeyId", "aws.secretAccessKey"),
        ZIO.unit
      ),
      test("no cred in environment properties") {
        for {
          failure <- ZIO.scoped[Any](env).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in profile") {
        for {
          failure <- ZIO.scoped[Any](profile).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in named profile") {
        for {
          failure <- ZIO.scoped[Any](profile(Some("name"))).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in container") {
        for {
          failure <- ZIO.scoped[Any](container).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in instance profile credentials") {
        for {
          failure <- ZIO.scoped[Any](instanceProfile).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in webidentity credentials") {
        for {
          failure <- ZIO.scoped[Any](webIdentity).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("settings from invalid creds") {
        for {
          failure <- ZIO
                       .scoped[Any](
                         settings(
                           Region.AF_SOUTH_1,
                           ZIO.scoped[Any](system).map(_.resolveCredentials())
                         ).build
                       )
                       .flip
        } yield assert(failure.getMessage)(isNonEmptyString)
      },
      test("no cred when chain all providers") {
        for {
          failure <- ZIO.scoped[Any](default.flatMap(c => ZIO.attempt(c.resolveCredentials()))).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      }
    ) @@ sequential
}

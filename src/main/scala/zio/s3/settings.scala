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

import software.amazon.awssdk.regions.Region
import zio.{ IO, ZIO }

final case class S3Credentials(accessKeyId: String, secretAccessKey: String)

sealed trait S3Region {
  val region: Region
}

object S3Region {

  def fromRegion(region: Region): Either[InvalidSettings, S3Region] =
    region match {
      case r if Region.regions().contains(r) => Right(new S3Region { val region = r })
      case r                                 => Left(InvalidSettings(s"Invalid aws region provided : ${r.id}"))
    }

    val AP_SOUTH_1 = "ap-south-1"

    val EU_SOUTH_1 = "eu-south-1"

    val US_GOV_EAST_1 = "us-gov-east-1"

    val CA_CENTRAL_1 = "ca-central-1"

    val EU_CENTRAL_1 = "eu-central-1"

    val US_WEST_1 = "us-west-1"

    val US_WEST_2 = "us-west-2"

    val AF_SOUTH_1 = "af-south-1"

    val EU_NORTH_1 = "eu-north-1"

    val EU_WEST_3 = "eu-west-3"

    val EU_WEST_2 = "eu-west-2"

    val EU_WEST_1 = "eu-west-1"

    val AP_NORTHEAST_2 = "ap-northeast-2"

    val AP_NORTHEAST_1 = "ap-northeast-1"

    val ME_SOUTH_1 = "me-south-1"

    val SA_EAST_1 = "sa-east-1"

    val AP_EAST_1 = "ap-east-1"

    val CN_NORTH_1 = "cn-north-1"

    val US_GOV_WEST_1 = "us-gov-west-1"

    val AP_SOUTHEAST_1 = "ap-southeast-1"

    val AP_SOUTHEAST_2 = "ap-southeast-2"

    val US_ISO_EAST_1 = "us-iso-east-1"

    val US_EAST_1 = "us-east-1"

    val US_EAST_2 = "us-east-2"

    val CN_NORTHWEST_1 = "cn-northwest-1"

    val US_ISOB_EAST_1 = "us-isob-east-1"

    val AWS_GLOBAL = "aws-global"

    val AWS_CN_GLOBAL = "aws-cn-global"

    val AWS_US_GOV_GLOBAL = "aws-us-gov-global"

    val AWS_ISO_GLOBAL = "aws-iso-global"

    val AWS_ISO_B_GLOBAL = "aws-iso-b-global"
  
  def fromString(value: String): Either[InvalidSettings, S3Region] = fromRegion(Region.of(value))
}

final case class S3Settings(s3Region: S3Region, credentials: S3Credentials)

object S3Settings {

  def from(region: String, credentials: S3Credentials): IO[InvalidSettings, S3Settings] =
    ZIO.fromEither(S3Region.fromString(region)).map(S3Settings(_, credentials))
}

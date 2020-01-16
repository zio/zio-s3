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

import java.nio.ByteBuffer

import zio.stream.ZStream
import zio.{ Chunk, ZIO }

/*
Need to be removed when ZIO-RC18 is released
 */
object ChunkUtils {

  def fromByteBuffer(buffer: ByteBuffer): Chunk[Byte] = {
    val dest = Array.ofDim[Byte](buffer.remaining())
    val pos  = buffer.position()
    buffer.get(dest)
    buffer.position(pos)
    Chunk.fromArray(dest)
  }
}

/*
Need to be removed when ZIO-RC18 is released
 */
object ZIOStreamUtils {

  def accessStream[R]: AccessStreamPartiallyApplied[R] =
    new AccessStreamPartiallyApplied[R]

  final class AccessStreamPartiallyApplied[R](private val dummy: Boolean = true) extends AnyVal {

    def apply[E, A](f: R => ZStream[R, E, A]): ZStream[R, E, A] =
      environment[R].flatMap(f)
  }

  def environment[R]: ZStream[R, Nothing, R] =
    ZStream.fromEffect(ZIO.environment[R])
}

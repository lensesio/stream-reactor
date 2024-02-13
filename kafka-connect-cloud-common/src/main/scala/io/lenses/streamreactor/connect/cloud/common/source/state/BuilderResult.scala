package io.lenses.streamreactor.connect.cloud.common.source.state

import cats.effect.IO
import cats.effect.kernel.Ref

object BuilderResult{

}
case class BuilderResult(
  state:                  CloudSourceTaskState,
  cancelledRef:           Ref[IO, Boolean],
  partitionDiscoveryLoop: IO[Unit],
)

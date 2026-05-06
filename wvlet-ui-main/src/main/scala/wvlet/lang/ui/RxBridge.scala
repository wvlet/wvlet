/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.ui

import scala.concurrent.{ExecutionContext, Promise}
import wvlet.airframe.rx.Rx as AirframeRx
import wvlet.uni.rx.Rx as UniRx

/**
  * Lift a one-shot uni-rx `Rx[A]` (what the migrated `FrontendRPC` async client returns) into an
  * airframe-rx `Rx[A]` so it can be embedded into airframe-rx-html templates via `embedAsNode`. uni
  * does not yet ship an rx-html replacement; until it does, the UI layer keeps using
  * `airframe-rx-html` and converts at the RPC boundary here.
  *
  * Conversion is one-shot: the bridge subscribes to the uni `Rx` exactly once and forwards the
  * first emitted value (or failure) to a `Promise`, which is then exposed as an airframe `Rx[A]`
  * via [[wvlet.airframe.rx.Rx.future]]. This matches RPC call semantics — every method on the
  * generated client emits one value or one error.
  */
object RxBridge:
  def toAirframe[A](u: UniRx[A])(using ec: ExecutionContext): AirframeRx[A] =
    val p = Promise[A]()
    u.tap { a =>
        if !p.isCompleted then
          p.success(a)
      }
      .tapOnFailure { e =>
        if !p.isCompleted then
          p.failure(e)
      }
      .run()
    AirframeRx.future(p.future)

end RxBridge

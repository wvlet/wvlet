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
package wvlet.lang.runner.compat

/**
  * Cross-platform synchronous sleep, used by polling loops (e.g. the remote wvlet-server client).
  * JVM and Native use `Thread.sleep`; Node.js has no thread sleep, so its impl blocks on
  * `Atomics.wait` (the same primitive uni's Node sync HTTP channel relies on).
  */
private[runner] object Sleep extends SleepCompatImpl

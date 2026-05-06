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
package wvlet.lang.api.v1.frontend.client

import wvlet.lang.api.v1.frontend.FileApi
import wvlet.lang.api.v1.frontend.FileApi.{FileRequest, SaveFileRequest}
import wvlet.lang.api.v1.io.FileEntry
import wvlet.uni.http.*
import wvlet.uni.http.rpc.RPCClient
import wvlet.uni.rx.Rx
import wvlet.uni.surface.Surface

/**
  * Hand-written RPC client for [[FileApi]]. Same shape as uni's codegen output — see
  * [[FrontendApiClient]] for rationale.
  */
object FileApiClient:
  private val rpc = RPCClient.build(Surface.of[FileApi], Surface.methodsOf[FileApi])

  class SyncClient(client: HttpSyncClient):
    def listFiles(request: FileRequest): List[FileEntry] = rpc.callSync(
      client,
      "listFiles",
      Seq(request)
    )

    def getFile(request: FileRequest): FileEntry = rpc.callSync(client, "getFile", Seq(request))
    def getPath(request: FileRequest): List[FileEntry] = rpc.callSync(
      client,
      "getPath",
      Seq(request)
    )

    def readFile(request: FileRequest): FileEntry = rpc.callSync(client, "readFile", Seq(request))
    def saveFile(request: SaveFileRequest): Unit  = rpc.callSync(client, "saveFile", Seq(request))
  end SyncClient

  class AsyncClient(client: HttpAsyncClient):
    def listFiles(request: FileRequest): Rx[List[FileEntry]] = rpc.callAsync(
      client,
      "listFiles",
      Seq(request)
    )

    def getFile(request: FileRequest): Rx[FileEntry] = rpc.callAsync(
      client,
      "getFile",
      Seq(request)
    )

    def getPath(request: FileRequest): Rx[List[FileEntry]] = rpc.callAsync(
      client,
      "getPath",
      Seq(request)
    )

    def readFile(request: FileRequest): Rx[FileEntry] = rpc.callAsync(
      client,
      "readFile",
      Seq(request)
    )

    def saveFile(request: SaveFileRequest): Rx[Unit] = rpc.callAsync(
      client,
      "saveFile",
      Seq(request)
    )

  end AsyncClient

end FileApiClient

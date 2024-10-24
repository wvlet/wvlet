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
package wvlet.lang.api.v1.frontend

import wvlet.airframe.http.{RPC, RxRouter, RxRouterProvider}
import wvlet.airframe.metrics.ElapsedTime
import wvlet.airframe.ulid.{PrefixedULID, ULID}
import wvlet.lang.BuildInfo
import wvlet.lang.api.v1.query.{QueryInfo, QueryStatus}

@RPC
trait FrontendApi:
  import FrontendApi.*

  def status: ServerStatus

  /**
    * Submit a query to from the frontend, and issue a new query id
    * @param request
    * @return
    */
  def submitQuery(request: QueryRequest): QueryResponse

  /**
    * Read the query status and partial results
    * @param queryId
    * @return
    */
  def getQueryInfo(request: QueryInfoRequest): QueryInfo

object FrontendApi extends RxRouterProvider:
  override def router = RxRouter.of[FrontendApi]

  case class ServerStatus(version: String = BuildInfo.version, upTime: ElapsedTime)

  case class QueryRequest(
      // wvlet query text
      query: String,
      profile: Option[String] = None,
      schema: Option[String] = None,
      isDebugRun: Boolean = true,
      requestId: ULID = ULID.newULID
  )

  case class QueryInfoRequest(queryId: ULID, pageToken: String)

  case class QueryResponse(queryId: ULID, requestId: ULID)

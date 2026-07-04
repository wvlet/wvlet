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
package wvlet.lang.ui.component.flow

import wvlet.lang.api.v1.frontend.FrontendRPC
import wvlet.lang.ui.component.MainFrame
import wvlet.uni.http.Http
import wvlet.uni.test.UniTest

class FlowRunsPageTest extends UniTest:
  test("render the flow runs page") {
    val client = FrontendRPC.newRPCAsyncClient(Http.client.newAsyncClient)
    val page   = FlowRunsPage(client)
    // Rendering succeeds without a live server; the initial list load fails asynchronously
    // and is reported through the in-page error banner instead of crashing
    page.render
  }

  test("switch the current page from the nav bar state") {
    MainFrame.currentPage := MainFrame.PAGE_FLOW_RUNS
    MainFrame.currentPage.get shouldBe MainFrame.PAGE_FLOW_RUNS
    MainFrame.currentPage := MainFrame.PAGE_EDITOR
    MainFrame.currentPage.get shouldBe MainFrame.PAGE_EDITOR
  }

end FlowRunsPageTest

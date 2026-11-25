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

// uni-ui's component styles (copied from the uni-ui jar by sbt-uni after each compile), then the
// playground's own layout
import './target/uni-ui.css'
import './index.css'

import { MonacoEditor } from '../wvlet-ui-main/src/main/scala/wvlet/lang/ui/component/monaco/MonacoEditor.ts'
import '../wvlet-ui-main/src/main/scala/wvlet/lang/ui/component/monaco/WvletLanguage.ts'

// Make the MonacoEditor class accessible from @JSGlobal in Scala.js
window.MonacoEditor = MonacoEditor;

// Start Scala.js code. Queries run on Wvlet's columnar engine in the browser; no DuckDB-Wasm.
import('__target__/main.js')

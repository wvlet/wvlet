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

// Import Tailwind CSS
import './index.css'

// Typescript
import {DuckDBWasm} from './src/main/scala/wvlet/lang/ui/playground/DuckDBWasm.ts'
window.duckdb = DuckDBWasm

import * as arrow from 'apache-arrow'
window.arrow = arrow

import { MonacoEditor } from './src/main/scala/wvlet/lang/ui/playground/MonacoEditor.ts'
import './src/main/scala/wvlet/lang/ui/playground/WvletLanguage.ts'

// Ensure that Monaco is loaded before assigning it to the window object
document.addEventListener('DOMContentLoaded', () => {
    import('monaco-editor').then((monaco) => {
        console.log("Monaco Editor loaded successfully");

        // Make the MonacoEditor class accessible from @JSGlobal in Scala.js
        window.MonacoEditor = MonacoEditor;

        // Start Scala.js code
        import('__target__/main.js')
    }).catch((error) => {
        console.error('Failed to load Monaco Editor:', error);
    });
});

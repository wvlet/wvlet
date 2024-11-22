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

// Scala.js code
import '__target__/main.js'

// Using ES6 import syntax
import hljs from 'highlight.js/lib/core';
import 'highlight.js/styles/atom-one-dark.css';
import './custom.css';
import sql from 'highlight.js/lib/languages/sql';

// Then register the languages you need
hljs.registerLanguage('sql', sql);
hljs.highlightAll();

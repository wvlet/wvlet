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

import { defineConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";
import fs from 'fs'
import replace from '@rollup/plugin-replace';

function isDev() {
  return process.env.NODE_ENV !== "production";
}

const scalaVersion = fs.readFileSync("../SCALA_VERSION").toString().trim();
const suffix = isDev() ? "-fastopt" : "-opt";
// sbt 2 emits Scala.js linker output under the root's <root>/target/out/sjs1/scala-<ver>/<proj>/
// tree instead of per-project target/scala-<ver>/. Point Vite at the new location.
const scalaJsTarget = `../target/out/sjs1/scala-${scalaVersion}/wvlet-ui-main/wvlet-ui-main${suffix}`;

export default defineConfig({
  server: {
    open: true,
    proxy: {
      '^/wvlet.lang.api.v1*': 'http://127.0.0.1:9090'
    }
  },
  plugins: [
    tailwindcss(),
    replace({
      preventAssignment: true,
      __target__: scalaJsTarget
    })
  ]
});

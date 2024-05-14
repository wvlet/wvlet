import { defineConfig } from "vite";
import fs from 'fs'
import replace from '@rollup/plugin-replace';

function isDev() {
return process.env.NODE_ENV !== "production";
}

const scalaVersion = fs.readFileSync("../SCALA_VERSION").toString().trim();
const suffix = isDev() ? "-fastopt" : "-opt";
const replacementForPublic= `./target/scala-${scalaVersion}/flow-ui-main${suffix}`;

export default defineConfig({
  server: {
  open: true,
  proxy: {
    '^/v1/*': 'http://127.0.0.1:9090',
  }
  },
  plugins: [
    replace({
      __public__: replacementForPublic
    })
  ]
});

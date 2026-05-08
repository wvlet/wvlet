#!/usr/bin/env node
// Thin shebang wrapper. Importing `lib/main.js` triggers the Scala.js main module
// initializer, which reads `process.argv` and dispatches through the wvlet CLI launcher.
import "../lib/main.js";

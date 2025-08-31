const fs = require('fs');
const path = require('path');

// Read the source file
const source = fs.readFileSync(path.join(__dirname, 'src/languages/wvlet.js'), 'utf8');

// Convert ES6 module to UMD format for browser compatibility
const umdWrapper = `(function (global, factory) {
  if (typeof exports === 'object' && typeof module !== 'undefined') {
    module.exports = factory();
  } else if (typeof define === 'function' && define.amd) {
    define(factory);
  } else {
    global.hljsDefineWvlet = factory();
  }
}(typeof self !== 'undefined' ? self : this, function () {
  ${source.replace('export default', 'return')}
}));`;

// Ensure dist directory exists
if (!fs.existsSync(path.join(__dirname, 'dist'))) {
  fs.mkdirSync(path.join(__dirname, 'dist'));
}

// Write the UMD version
fs.writeFileSync(path.join(__dirname, 'dist/wvlet.js'), umdWrapper);

// Create a minified version (basic minification)
const minified = umdWrapper
  .replace(/\/\*[\s\S]*?\*\//g, '') // Remove block comments
  .replace(/\/\/.*$/gm, '') // Remove line comments
  .replace(/\s+/g, ' ') // Collapse whitespace
  .replace(/\s*([{}();,:])\s*/g, '$1') // Remove spaces around syntax
  .trim();

fs.writeFileSync(path.join(__dirname, 'dist/wvlet.min.js'), minified);

console.log('Build complete! Files created:');
console.log('  - dist/wvlet.js');
console.log('  - dist/wvlet.min.js');
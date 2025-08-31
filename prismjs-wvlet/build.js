const fs = require('fs');
const path = require('path');
const { minify } = require('terser');

async function build() {
  // Read the component file
  const componentSource = fs.readFileSync(
    path.join(__dirname, 'components/prism-wvlet.js'), 
    'utf8'
  );

  // UMD wrapper for browser compatibility
  const umdWrapper = `/*!
 * Prism.js Wvlet Language Definition
 * https://wvlet.org
 * Licensed under Apache 2.0
 */
(function (Prism) {
  'use strict';

  ${componentSource.replace(/^\/\*[\s\S]*?\*\/\s*/, '')}

})(typeof global !== 'undefined' ? global.Prism : typeof window !== 'undefined' ? window.Prism : typeof self !== 'undefined' ? self.Prism : {});`;

  // Create dist directory
  if (!fs.existsSync(path.join(__dirname, 'dist'))) {
    fs.mkdirSync(path.join(__dirname, 'dist'), { recursive: true });
  }

  // Write unminified version
  fs.writeFileSync(path.join(__dirname, 'dist/prism-wvlet.js'), umdWrapper);

  // Create minified version
  const minified = await minify(umdWrapper, {
    compress: true,
    mangle: false, // Preserve token names for debugging
    format: {
      comments: /^!/
    }
  });

  fs.writeFileSync(
    path.join(__dirname, 'dist/prism-wvlet.min.js'),
    minified.code
  );

  console.log('Build complete! Files created:');
  console.log('  - dist/prism-wvlet.js');
  console.log('  - dist/prism-wvlet.min.js');
}

build().catch(console.error);
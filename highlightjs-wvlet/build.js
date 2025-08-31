const fs = require('fs');
const path = require('path');
const { minify } = require('terser');

async function build() {
  try {
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
      fs.mkdirSync(path.join(__dirname, 'dist'), { recursive: true });
    }

    // Write the UMD version
    fs.writeFileSync(path.join(__dirname, 'dist/wvlet.js'), umdWrapper);

    // Create a minified version using terser
    const minified = await minify(umdWrapper, {
      compress: {
        dead_code: true,
        drop_console: false,
        drop_debugger: true,
        keep_fargs: false,
        keep_fnames: false,
        keep_infinity: false,
        passes: 2
      },
      mangle: {
        keep_fnames: false,
        toplevel: false
      },
      format: {
        comments: false,
        preamble: '/*! Wvlet syntax highlighting for highlight.js */'
      }
    });

    if (minified.error) {
      throw minified.error;
    }

    fs.writeFileSync(path.join(__dirname, 'dist/wvlet.min.js'), minified.code);

    console.log('Build complete! Files created:');
    console.log('  - dist/wvlet.js');
    console.log('  - dist/wvlet.min.js');
  } catch (error) {
    console.error('Build failed:', error);
    process.exit(1);
  }
}

// Run the build
build();
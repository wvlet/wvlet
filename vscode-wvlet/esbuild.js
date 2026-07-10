const esbuild = require('esbuild');

const isWatch = process.argv.includes('--watch');

// The Scala.js compiler bundle (sdks/typescript/lib/main.js) statically imports
// `koffi`, the native FFI used only by the DuckDB query connector. The language
// server never executes queries, so that binding is never referenced. Bundling
// koffi as CJS breaks it (its `createRequire(import.meta.url)` resolves to
// undefined, and its native `.node` addons cannot be inlined). Replace koffi
// with an empty stub so the bundle stays self-contained and side-effect free.
const stubKoffiPlugin = {
  name: 'stub-koffi',
  setup(build) {
    build.onResolve({ filter: /^koffi$/ }, () => ({
      path: 'koffi',
      namespace: 'stub-koffi',
    }));
    build.onLoad({ filter: /.*/, namespace: 'stub-koffi' }, () => ({
      contents: 'export default {};',
      loader: 'js',
    }));
  },
};

/** @type {import('esbuild').BuildOptions} */
const extensionConfig = {
  entryPoints: ['src/extension.ts'],
  bundle: true,
  outfile: 'out/extension.js',
  external: ['vscode'],
  format: 'cjs',
  platform: 'node',
  target: 'node18',
  sourcemap: true,
};

/** @type {import('esbuild').BuildOptions} */
const serverConfig = {
  entryPoints: ['src/server.ts'],
  bundle: true,
  outfile: 'out/server.js',
  format: 'cjs',
  platform: 'node',
  target: 'node18',
  sourcemap: true,
  plugins: [stubKoffiPlugin],
};

async function build() {
  if (isWatch) {
    const extCtx = await esbuild.context(extensionConfig);
    const srvCtx = await esbuild.context(serverConfig);
    await Promise.all([extCtx.watch(), srvCtx.watch()]);
    console.log('Watching for changes...');
  } else {
    await Promise.all([
      esbuild.build(extensionConfig),
      esbuild.build(serverConfig),
    ]);
    console.log('Build complete.');
  }
}

build().catch((e) => {
  console.error(e);
  process.exit(1);
});

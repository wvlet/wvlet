// LSP smoke test for the packaged extension (#1887 follow-up).
//
// Drives the given server bundle over real LSP stdio — the same protocol and
// artifact the editor uses — so a packaging regression (missing compiler bundle,
// broken module resolution) fails the release instead of shipping silently dead,
// as happened with 2026.1.0.
//
// Usage: node scripts/lsp-smoke.mjs <path/to/out/server.js>

import { spawn } from 'node:child_process';

const serverPath = process.argv[2];
if (!serverPath) {
  console.error('usage: node scripts/lsp-smoke.mjs <path/to/server.js>');
  process.exit(2);
}

const server = spawn('node', [serverPath, '--stdio']);
let buf = Buffer.alloc(0);
const pending = new Map();
let nextId = 1;
// Set before every intentional termination; any other server exit is a failure
let finished = false;

const timeout = setTimeout(() => {
  console.error('LSP smoke test timed out after 60s');
  finished = true;
  server.kill();
  process.exit(1);
}, 60_000);

server.stderr.on('data', (d) => process.stderr.write(d));
server.on('exit', (code) => {
  if (!finished) {
    console.error(`server exited unexpectedly (code ${code})`);
    process.exit(1);
  }
});

server.stdout.on('data', (d) => {
  buf = Buffer.concat([buf, d]);
  for (;;) {
    const headerEnd = buf.indexOf('\r\n\r\n');
    if (headerEnd < 0) {
      return;
    }
    const header = buf.slice(0, headerEnd).toString();
    const len = parseInt(/Content-Length: (\d+)/.exec(header)[1], 10);
    if (buf.length < headerEnd + 4 + len) {
      return;
    }
    const msg = JSON.parse(buf.slice(headerEnd + 4, headerEnd + 4 + len).toString());
    buf = buf.slice(headerEnd + 4 + len);
    if (msg.id !== undefined && pending.has(msg.id)) {
      pending.get(msg.id)(msg);
      pending.delete(msg.id);
    }
  }
});

function send(obj) {
  const s = JSON.stringify(obj);
  server.stdin.write(`Content-Length: ${Buffer.byteLength(s)}\r\n\r\n${s}`);
}

function request(method, params) {
  return new Promise((resolve) => {
    const id = nextId++;
    pending.set(id, resolve);
    send({ jsonrpc: '2.0', id, method, params });
  });
}

const failures = [];
function check(name, condition, detail) {
  if (condition) {
    console.log(`ok: ${name}`);
  } else {
    failures.push(name);
    console.error(`FAIL: ${name}${detail ? ` — ${detail}` : ''}`);
  }
}

const init = await request('initialize', { processId: null, rootUri: null, capabilities: {} });
const caps = init.result?.capabilities ?? {};
check('initialize returns capabilities', init.result !== undefined);
check('definition provider advertised', caps.definitionProvider === true);
check('hover provider advertised', caps.hoverProvider === true);
check(
  'dot completion trigger advertised',
  (caps.completionProvider?.triggerCharacters ?? []).includes('.'),
  JSON.stringify(caps.completionProvider)
);
send({ jsonrpc: '2.0', method: 'initialized', params: {} });

const uri = 'file:///smoke/check.wv';
const text = 'model m1 = {\n  from [[1, 2]] as t(a, b)\n}\n\nfrom m1\nselect a\n';
send({
  jsonrpc: '2.0',
  method: 'textDocument/didOpen',
  params: { textDocument: { uri, languageId: 'wvlet', version: 1, text } },
});

// Cursor on the `m1` reference (0-based line 4) must resolve to the model definition
const def = await request('textDocument/definition', {
  textDocument: { uri },
  position: { line: 4, character: 6 },
});
check('go-to-definition resolves a model reference', def.result?.range?.start?.line === 0, JSON.stringify(def.result));

// Hover on column `a` in the projection must report its type from the model schema
const hover = await request('textDocument/hover', {
  textDocument: { uri },
  position: { line: 5, character: 8 },
});
const hoverText = JSON.stringify(hover.result?.contents ?? '');
check('hover types a column through the model', hoverText.includes('long'), hoverText.slice(0, 120));

clearTimeout(timeout);
finished = true;
server.kill();
if (failures.length > 0) {
  console.error(`LSP smoke test failed: ${failures.length} check(s)`);
  process.exit(1);
}
console.log('LSP smoke test passed');
process.exit(0);

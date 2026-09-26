// Host process for Wvlet external functions implemented in TypeScript/JavaScript modules.
//
//   node wvlet-udf-host.mjs list   <module>...          -> {"<export>": "<module>", ...}
//   node wvlet-udf-host.mjs table  <fn> <module>...     -> result object on stdout
//   node wvlet-udf-host.mjs scalar <fn> <module>...     -> JSON-lines rows on stdout
//
// Input rows arrive on stdin as JSON lines. Call arguments are in WVLET_FUNCTION_ARGS (a JSON
// object by parameter name). In scalar mode WVLET_ARG_COLUMNS (JSON array) names the input columns
// holding the per-row arguments, and WVLET_OUTPUT_COLUMN names the column that receives the result.
import { createInterface } from 'node:readline'
import { pathToFileURL } from 'node:url'

const [mode, ...rest] = process.argv.slice(2)

function fail(message) {
  process.stderr.write(`${message}\n`)
  process.exit(2)
}

async function loadModules(paths) {
  const modules = []
  for (const p of paths) {
    modules.push([p, await import(pathToFileURL(p).href)])
  }
  return modules
}

async function* inputRows() {
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity })
  for await (const line of lines) {
    if (line.trim().length > 0) yield JSON.parse(line)
  }
}

function write(text) {
  return new Promise((resolve, reject) =>
    process.stdout.write(text, (err) => (err ? reject(err) : resolve()))
  )
}

if (mode === 'list') {
  const exports = {}
  for (const [path, mod] of await loadModules(rest)) {
    for (const [name, value] of Object.entries(mod)) {
      if (typeof value !== 'function') continue
      if (name in exports) fail(`Function '${name}' is exported by both ${exports[name]} and ${path}`)
      exports[name] = path
    }
  }
  await write(JSON.stringify(exports))
} else if (mode === 'table' || mode === 'scalar') {
  const [fnName, ...modulePaths] = rest
  const found = (await loadModules(modulePaths)).filter(([, mod]) => typeof mod[fnName] === 'function')
  if (found.length === 0) fail(`Function '${fnName}' is not exported by: ${modulePaths.join(', ')}`)
  if (found.length > 1) fail(`Function '${fnName}' is exported by multiple modules: ${found.map(([p]) => p).join(', ')}`)
  const fn = found[0][1][fnName]
  const args = JSON.parse(process.env.WVLET_FUNCTION_ARGS ?? '{}')

  if (mode === 'table') {
    const result = await fn(inputRows(), args)
    if (result === null || typeof result !== 'object' || Array.isArray(result)) {
      fail(`Table function '${fnName}' must return an object (e.g. { rows: [...] }), got: ${JSON.stringify(result)}`)
    }
    await write(JSON.stringify(result))
  } else {
    const argColumns = JSON.parse(process.env.WVLET_ARG_COLUMNS ?? '[]')
    const outputColumn = process.env.WVLET_OUTPUT_COLUMN ?? 'result'
    for await (const row of inputRows()) {
      const value = await fn(...argColumns.map((c) => row[c]))
      for (const c of argColumns) delete row[c]
      row[outputColumn] = value === undefined ? null : value
      await write(`${JSON.stringify(row)}\n`)
    }
  }
} else {
  fail(`Unknown mode '${mode}'. Expected one of: list, table, scalar`)
}

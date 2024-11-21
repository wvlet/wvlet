import * as duckdb from "@duckdb/duckdb-wasm";

// Select a bundle based on browser checks
export async function initDuckDB(){
    const JSDELIVER_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVER_BUNDLES);

    const worker_url = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: 'application/javascript'
    }));

    // Instantiate the asynchronus version of DuckDB-wasm
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(worker_url);
    return db;
}


export default function hello() {
    console.log("Hello from duckdb.js");
}

const db = await initDuckDB();
const c = await db.connect();
const r = await c.query("SELECT 'hello duckdb' as msg")
console.log(r.toString());

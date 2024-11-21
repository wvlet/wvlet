import * as duckdb from "@duckdb/duckdb-wasm";

export class DuckDB {
    private duckdb: Promise<duckdb.AsyncDuckDB>;

    constructor() {
        this.duckdb = DuckDB.initDuckDB();
    }

    static async initDuckDB(): Promise<duckdb.AsyncDuckDB> {
        const JSDELIVER_BUNDLES = duckdb.getJsDelivrBundles();
        const bundle = await duckdb.selectBundle(JSDELIVER_BUNDLES);

        const worker_url = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], {
            type: 'application/javascript'
        }));

        // Instantiate the asynchronous version of DuckDB-wasm
        const worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger();
        const db = new duckdb.AsyncDuckDB(logger, worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        URL.revokeObjectURL(worker_url);
        console.log("Initialized DuckDB-Wasm");
        return db;
    }

    async query(sql: string): Promise<string> {
        const db = await this.duckdb;
        const c = await db.connect();
        try {
            const r = await c.query(sql);
            const result = r.toArray().map((row) => row.toJSON());
            return JSON.stringify(result);
        } finally {
            c.close();
        }
    }
}

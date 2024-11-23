import * as duckdb from "@duckdb/duckdb-wasm";

export class DuckDBWasm {
    private duckdb: Promise<duckdb.AsyncDuckDB>;
    /**
     * Keep the same connection to preserve the in-memory tables
     * @private
     */
    private conn: Promise<duckdb.AsyncDuckDBConnection>;

    constructor() {
        this.duckdb = DuckDBWasm.initDuckDB();
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

    async connect(): Promise<duckdb.AsyncDuckDBConnection> {
        if(!this.conn) {
            const db = await this.duckdb;
            this.conn = db.connect();
        }
        return this.conn;
    }

    async query(sql: string): Promise<string> {
        const c = await this.connect();
        const r = await c.query(sql);
        const result = r.toArray().map((row) => row.toJSON());
        // console.log(`Query result: ${r.numRows} rows`);
        return JSON.stringify(result,
            // Woarkaround for 'Do not know how to serialize a BigInt' error
            (key, value) =>
            typeof value === "bigint" ? Number(value) : value,
        );
    }

    async close() {
        if(this.conn) {
            const c = await this.conn;
            c.close();
        }
    }
}

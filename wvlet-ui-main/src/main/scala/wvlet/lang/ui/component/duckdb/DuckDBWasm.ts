import * as duckdb from "@duckdb/duckdb-wasm";
import {Table} from 'apache-arrow'

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

    async query(sql: string): Promise<Table> {
        const c = await this.connect();
        const r = await c.query(sql);
        return r;
    }

    async close() {
        if(this.conn) {
            const c = await this.conn;
            c.close();
        }
    }
}


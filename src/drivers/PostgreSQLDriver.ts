import { Pool, PoolClient, PoolConfig } from 'pg';
import { IDriver, Query, Data } from '../interfaces/IDriver';

/**
 * PostgreSQL driver configuration options
 */
export interface PostgreSQLDriverOptions {
    /** PostgreSQL connection string (e.g. 'postgres://user:pass@host:port/db') */
    connectionString?: string;
    /** Database host */
    host?: string;
    /** Database port (default: 5432) */
    port?: number;
    /** Database user */
    user?: string;
    /** Database password */
    password?: string;
    /** Database name */
    database?: string;
    /** Table name to use for storing data */
    tableName: string;
    /** SSL configuration (required for some cloud providers) */
    ssl?: boolean | object;
    /** Connection pool size (default: 10) */
    max?: number;
    /** Additional pool configuration */
    poolOptions?: PoolConfig;
}

/**
 * Driver implementation for PostgreSQL using node-postgres.
 * Supports Hybrid Schema (Real Columns + JSONB) for optimized storage and querying.
 */
export class PostgreSQLDriver implements IDriver {
    private pool: Pool | null = null;
    private tableName: string;
    private config: PoolConfig;
    private schema: any = null;

    /**
     * Database type (sql or nosql).
     * Defaults to 'nosql' until a schema is set via setSchema().
     */
    public dbType: 'sql' | 'nosql' = 'nosql';

    /**
     * Creates a new instance of PostgreSQLDriver
     * @param options - PostgreSQL driver configuration options
     */
    constructor(options: PostgreSQLDriverOptions) {
        this.tableName = options.tableName;

        this.config = {
            connectionString: options.connectionString,
            host: options.host,
            port: options.port,
            user: options.user,
            password: options.password,
            database: options.database,
            max: options.max || 10,
            ssl: options.ssl !== undefined ? options.ssl : (options.connectionString?.includes('sslmode=require') ? { rejectUnauthorized: false } : false),
            ...options.poolOptions,
        };
    }

    /**
     * Sets the schema for the driver.
     * Switches the driver to SQL mode to use real columns.
     * @param schema - The schema instance
     */
    setSchema(schema: any): void {
        this.schema = schema;
        this.dbType = 'sql';
    }

    /**
     * Connects to the PostgreSQL database.
     * Creates the table (Hybrid or JSONB only) if it doesn't exist.
     */
    async connect(): Promise<void> {
        this.pool = new Pool(this.config);

        let createTableSQL = '';

        if (this.schema && this.dbType === 'sql') {
            const definition = typeof this.schema.getDefinition === 'function'
                ? this.schema.getDefinition()
                : this.schema;

            // Map schema fields to Postgres column definitions
            const columns = Object.entries(definition).map(([key, type]) => {
                const sqlType = this.mapHawiahTypeToSQL(type);
                return `${key} ${sqlType}`; // e.g. "age INTEGER"
            }).join(', \n');

            // Hybrid Table: Real Columns + _extras JSONB
            createTableSQL = `
                CREATE TABLE IF NOT EXISTS ${this.tableName} (
                    _id VARCHAR(100) PRIMARY KEY,
                    ${columns}, 
                    _extras JSONB DEFAULT '{}',
                    _createdAt TIMESTAMP NOT NULL,
                    _updatedAt TIMESTAMP NOT NULL
                )
            `;
        } else {
            // NoSQL Mode: Everything stored in _data JSONB
            createTableSQL = `
                CREATE TABLE IF NOT EXISTS ${this.tableName} (
                    _id VARCHAR(100) PRIMARY KEY,
                    _data JSONB NOT NULL,
                    _createdAt TIMESTAMP NOT NULL,
                    _updatedAt TIMESTAMP NOT NULL
                )
            `;
        }

        await this.pool.query(createTableSQL);
        // Ensure standard indexes exist
        await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_${this.tableName}_createdAt ON ${this.tableName}(_createdAt)`);
        await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_${this.tableName}_updatedAt ON ${this.tableName}(_updatedAt)`);
    }

    /**
     * Disconnects from the PostgreSQL database.
     */
    async disconnect(): Promise<void> {
        if (this.pool) {
            await this.pool.end();
            this.pool = null;
        }
    }

    /**
     * Inserts a new record into the database.
     * Uses real columns if schema is present, otherwise stores as JSONB.
     * @param data - The data to insert
     * @returns The inserted record with ID
     */
    async set(data: Data): Promise<Data> {
        this.ensureConnected();

        const id = this.generateId();
        const now = new Date();
        const record = {
            ...data,
            _id: id,
            _createdAt: now.toISOString(),
            _updatedAt: now.toISOString(),
        };

        if (this.schema && this.dbType === 'sql') {
            const { schemaData, extraData } = this.splitData(record);
            const schemaKeys = Object.keys(schemaData);
            const schemaValues = Object.values(schemaData);

            // Columns: ID + Schema Fields + Extras + Times
            const cols = ['_id', ...schemaKeys, '_extras', '_createdAt', '_updatedAt'];
            // Placeholders: $1, $2, ...
            const valueParams = cols.map((_, i) => `$${i + 1}`).join(', ');
            
            // Values mapped to placeholders
            const values = [id, ...schemaValues, JSON.stringify(extraData), now, now];

            const sql = `INSERT INTO ${this.tableName} (${cols.join(', ')}) VALUES (${valueParams})`;
            await this.pool!.query(sql, values);

        } else {
            const sql = `
                INSERT INTO ${this.tableName} (_id, _data, _createdAt, _updatedAt)
                VALUES ($1, $2, $3, $4)
            `;
            await this.pool!.query(sql, [id, JSON.stringify(record), now, now]);
        }

        return record;
    }

    /**
     * Retrieves records matching the query.
     * Merges JSONB data with columns if in Hybrid mode.
     * @param query - The query criteria
     * @returns Array of matching records
     */
    async get(query: Query): Promise<Data[]> {
        this.ensureConnected();

        if (this.schema && this.dbType === 'sql') {
            const sql = `SELECT * FROM ${this.tableName}`;
            const result = await this.pool!.query(sql);

            // Merge Real Columns + Extras JSONB
            const records = result.rows.map(row => this.mergeData(row));

            if (Object.keys(query).length === 0) return records;
            return records.filter(record => this.matchesQuery(record, query));

        } else {
            const sql = `SELECT _data FROM ${this.tableName}`;
            const result = await this.pool!.query(sql);

            const allRecords = result.rows.map((row: any) => row._data);

            if (Object.keys(query).length === 0) return allRecords;
            return allRecords.filter((record: any) => this.matchesQuery(record, query));
        }
    }

    /**
     * Retrieves a single record matching the query.
     * @param query - The query criteria
     * @returns The first matching record or null
     */
    async getOne(query: Query): Promise<Data | null> {
        this.ensureConnected();

        if (query._id) {
            if (this.schema && this.dbType === 'sql') {
                const sql = `SELECT * FROM ${this.tableName} WHERE _id = $1 LIMIT 1`;
                const result = await this.pool!.query(sql, [query._id]);
                return result.rows.length > 0 ? this.mergeData(result.rows[0]) : null;
            } else {
                const sql = `SELECT _data FROM ${this.tableName} WHERE _id = $1 LIMIT 1`;
                const result = await this.pool!.query(sql, [query._id]);
                return result.rows.length > 0 ? result.rows[0]._data : null;
            }
        }

        const results = await this.get(query);
        return results.length > 0 ? results[0] : null;
    }

    /**
     * Updates records matching the query.
     * Updates both columns and extra JSONB data accordingly.
     * @param query - The query criteria
     * @param data - The data to update
     * @returns The number of updated records
     */
    async update(query: Query, data: Data): Promise<number> {
        this.ensureConnected();

        const records = await this.get(query);
        let count = 0;

        for (const record of records) {
            const updatedRecord: any = {
                ...record,
                ...data,
                _updatedAt: new Date().toISOString(),
            };

            const now = new Date();

            if (this.schema && this.dbType === 'sql') {
                const { schemaData, extraData } = this.splitData(updatedRecord);
                const schemaKeys = Object.keys(schemaData);
                const schemaValues = Object.values(schemaData);

                // Build dynamic SET clause: "col1 = $1, col2 = $2, ..."
                let setParts = [];
                let params = [];
                let idx = 1;

                for (let i = 0; i < schemaKeys.length; i++) {
                    setParts.push(`${schemaKeys[i]} = $${idx++}`);
                    params.push(schemaValues[i]);
                }

                setParts.push(`_extras = $${idx++}`);
                params.push(JSON.stringify(extraData));

                setParts.push(`_updatedAt = $${idx++}`);
                params.push(now);

                params.push(record._id); // Last param is ID

                const sql = `UPDATE ${this.tableName} SET ${setParts.join(', ')} WHERE _id = $${idx}`;
                await this.pool!.query(sql, params);

            } else {
                const sql = `UPDATE ${this.tableName} SET _data = $1, _updatedAt = $2 WHERE _id = $3`;
                await this.pool!.query(sql, [JSON.stringify(updatedRecord), now, record._id]);
            }
            count++;
        }

        return count;
    }

    /**
     * Deletes records matching the query.
     * @param query - The query criteria
     * @returns The number of deleted records
     */
    async delete(query: Query): Promise<number> {
        this.ensureConnected();
        const records = await this.get(query);
        const sql = `DELETE FROM ${this.tableName} WHERE _id = $1`;

        let count = 0;
        for (const record of records) {
            await this.pool!.query(sql, [record._id]);
            count++;
        }
        return count;
    }

    /**
     * Checks if any record matches the query.
     * @param query - The query criteria
     * @returns True if a match exists, false otherwise
     */
    async exists(query: Query): Promise<boolean> {
        this.ensureConnected();
        const result = await this.getOne(query);
        return result !== null;
    }

    /**
     * Counts records matching the query.
     * @param query - The query criteria
     * @returns The number of matching records
     */
    async count(query: Query): Promise<number> {
        this.ensureConnected();

        if (Object.keys(query).length === 0) {
            const sql = `SELECT COUNT(*) as count FROM ${this.tableName}`;
            const result = await this.pool!.query(sql);
            return parseInt(result.rows[0].count);
        }

        const results = await this.get(query);
        return results.length;
    }

    /**
     * Ensures the database is connected before executing operations.
     * @throws Error if database is not connected
     * @private
     */
    private ensureConnected(): void {
        if (!this.pool) {
            throw new Error('Database not connected. Call connect() first.');
        }
    }

    /**
     * Generates a unique ID for records.
     * @returns A unique string ID
     * @private
     */
    private generateId(): string {
        return `${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
    }

    /**
     * Checks if a record matches the query criteria.
     * @param record - The record to check
     * @param query - The query criteria
     * @returns True if the record matches
     * @private
     */
    private matchesQuery(record: Data, query: Query): boolean {
        for (const [key, value] of Object.entries(query)) {
            if (record[key] !== value) {
                return false;
            }
        }
        return true;
    }

    /**
     * Maps Hawiah types to Postgres types.
     * @param type - The Hawiah schema type
     * @returns The corresponding PostgreSQL type string
     * @private
     */
    private mapHawiahTypeToSQL(type: any): string {
        let t = type;
        if (typeof type === 'object' && type !== null && type.type) {
            t = type.type;
        }
        t = String(t).toUpperCase();

        if (t.includes('STRING') || t.includes('TEXT') || t.includes('EMAIL') || t.includes('URL') || t.includes('CHAR')) return 'TEXT';
        if (t.includes('UUID')) return 'UUID'; // Native UUID support
        if (t.includes('NUMBER')) {
            if (t.includes('INT')) return 'INTEGER';
            if (t.includes('BIGINT')) return 'BIGINT';
            return 'REAL'; // Default float
        }
        if (t.includes('BOOLEAN')) return 'BOOLEAN';
        if (t.includes('DATE')) return 'TIMESTAMP';
        if (t.includes('JSON')) return 'JSONB'; // Native JSON support
        if (t.includes('BLOB')) return 'BYTEA';

        return 'TEXT';
    }

    /**
     * Splits data into schema columns and extra data.
     * @param data - The full data object
     * @returns Object containing separate schemaData and extraData
     * @private
     */
    private splitData(data: Data): { schemaData: Data, extraData: Data } {
        if (!this.schema) return { schemaData: {}, extraData: data };

        const definition = typeof this.schema.getDefinition === 'function'
            ? this.schema.getDefinition()
            : this.schema;

        const schemaData: Data = {};
        const extraData: Data = {};

        for (const [key, value] of Object.entries(data)) {
            if (key in definition) {
                schemaData[key] = value;
            } else if (!['_id', '_createdAt', '_updatedAt'].includes(key)) {
                extraData[key] = value;
            }
        }
        return { schemaData, extraData };
    }

    /**
     * Merges schema columns and extra data back into a single object.
     * @param row - The raw database row
     * @returns The fully merged data object
     * @private
     */
    private mergeData(row: any): Data {
        const { _extras, ...rest } = row;
        // Postgres pg driver automatically parses JSONB columns into objects
        const extras = (typeof _extras === 'object' && _extras !== null) ? _extras : 
                       (typeof _extras === 'string' ? JSON.parse(_extras) : {});
                       
        return { ...rest, ...extras };
    }

    // --- Public Utility Methods ---

    /**
     * Gets the PostgreSQL connection pool.
     * @returns The PostgreSQL connection pool
     */
    getPool(): Pool | null { return this.pool; }

    /**
     * Executes a raw SQL query.
     * WARNING: Use with caution. This bypasses the abstraction layer.
     * @param sql - The SQL query to execute
     * @param params - Optional parameters for the query
     * @returns Query results (rows)
     */
    async executeRaw(sql: string, params?: any[]): Promise<any> {
        this.ensureConnected();
        const result = await this.pool!.query(sql, params);
        return result.rows;
    }

    /**
     * Clears all data from the table.
     */
    async clear(): Promise<void> {
        this.ensureConnected();
        await this.pool!.query(`DELETE FROM ${this.tableName}`);
    }

    /**
     * Drops the entire table.
     * WARNING: This will permanently delete all data and indexes.
     */
    async drop(): Promise<void> {
        this.ensureConnected();
        await this.pool!.query(`DROP TABLE IF EXISTS ${this.tableName}`);
    }

    /**
     * Vacuums the table for optimization.
     */
    async vacuum(): Promise<void> {
        this.ensureConnected();
        await this.pool!.query(`VACUUM ${this.tableName}`);
    }

    /**
     * Analyzes the table for query optimization.
     */
    async analyze(): Promise<void> {
        this.ensureConnected();
        await this.pool!.query(`ANALYZE ${this.tableName}`);
    }

    /**
     * Begins a transaction.
     * @returns A client from the pool for transaction use
     */
    async beginTransaction(): Promise<PoolClient> {
        this.ensureConnected();
        const client = await this.pool!.connect();
        await client.query('BEGIN');
        return client;
    }

    /**
     * Commits a transaction.
     * @param client - The client to commit
     */
    async commit(client: PoolClient): Promise<void> {
        await client.query('COMMIT');
        client.release();
    }

    /**
     * Rolls back a transaction.
     * @param client - The client to rollback
     */
    async rollback(client: PoolClient): Promise<void> {
        await client.query('ROLLBACK');
        client.release();
    }
}
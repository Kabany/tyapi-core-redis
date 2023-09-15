import { RedisClientType, createClient } from "redis";
import { Context, DatabaseService, TyapiError } from "tyapi-core";
import { StringUtils } from "tyapi-utils";

const DB_ENGINE = "DB_ENGINE";
const DB_ENGINE_TYPE = "REDIS";

const DB_HOST = "DB_HOST";
const DB_PORT = "DB_PORT";
const DB_USER = "DB_USER";
const DB_PASS = "DB_PASS";
const DB_SSL = "DB_SSL";

interface RedisParamsInterface {
  host?: string
  port?: number
  user?: string
  pass?: string
  ssl?: boolean
}


export class RedisService extends DatabaseService {

  protected connection: RedisClientType

  constructor(params: RedisParamsInterface, context: Context) {
    let map = new Map()
    map.set(DB_ENGINE, DB_ENGINE_TYPE)
    map.set(DB_HOST, params.host || "localhost");
    map.set(DB_PORT, params.port || 6379);
    map.set(DB_USER, params.user);
    map.set(DB_PASS, params.pass);
    map.set(DB_SSL, params.ssl || false);
    super(map, context);
  }
  
  override async connect() {
    this.getLogger()?.info(RedisService.name, `Creating a connection with Redis on ${this.params.get(DB_HOST)}:${this.params.get(DB_PORT)}...`);
    let url = `redis${this.params.get(DB_SSL) == true ? "s" : ""}://`
    if (StringUtils.isEmpty(this.params.get(DB_USER)) && StringUtils.isEmpty(this.params.get(DB_PASS))) {
      // do nothing
    } else {
      url += `${this.params.get(DB_USER) || "redis"}${StringUtils.isEmpty(this.params.get(DB_PASS)) ? "" : (":" + this.params.get(DB_PASS))}@`
    }
    url += `${this.params.get(DB_HOST)}:${this.params.get(DB_PORT)}`
    this.connection = createClient({url})
    await this.connection.connect()
    this.getLogger()?.success(RedisService.name, `Successfully connected to Redis ${this.params.get(DB_HOST)}:${this.params.get(DB_PORT)}.`);
  }

  override async disconnect() {
    this.getLogger()?.info(RedisService.name, `Disconnecting from Redis ${this.params.get(DB_HOST)}:${this.params.get(DB_PORT)}...`);
    if (this.connection != null) {
      await this.connection.disconnect()
    }
    this.getLogger()?.success(RedisService.name, `Successfully disconnected from Redis ${this.params.get(DB_HOST)}:${this.params.get(DB_PORT)}.`);
  }

  override async beforeMount() {
    let errors: any = {}
    if (!this.params.has(DB_ENGINE)) {
      errors.dbEngine = "Not defined";
    }
    if (!this.params.has(DB_HOST)) {
      errors.dbHost = "Not defined";
    }
    if (!this.params.has(DB_PORT)) {
      errors.dbPort = "Not defined";
    }
    if (!this.params.has(DB_USER)) {
      errors.dbUser = "Not defined";
    }
    if (!this.params.has(DB_PASS)) {
      errors.dbPass = "Not defined";
    }
    if (!this.params.has(DB_SSL)) {
      errors.dbName = "Not defined";
    }

    if (Object.keys(errors).length > 0) {
      let msg = "Error with db parameters:\n";
      for (let key of Object.keys(errors)) {
        msg += `${key}: ${errors[key]}\n`;
      }
      throw new TyapiError(msg);
    }
  }

  /**
   * Executes a GET or SET query for a simple value
   * @returns 
   */
  override async query(command: "get" | "set", key: string, value?: any, exp?: number) {
    if (command == "get") {
      return this.connection.get(key)
    } else {
      return this.connection.set(key, value, exp != null ? {EX: exp} : {})
    }
  }s

  /** Return the Redis Client */
  getClient() {
    return this.connection
  }

  /** Returns the schema name of the current connection. */
  override getDatabaseName() {
    return this.params.get(DB_HOST) as string;
  }
}





export class MultipleRedisService extends DatabaseService {
  protected dbs: Map<string, RedisService>
  constructor(context: Context) {
    let newMap: Map<string, any> = new Map();
    newMap.set(DB_ENGINE, "MULTI_REDIS");
    super(newMap, context);
    this.dbs = new Map();
  }

  override async beforeMount(): Promise<void> {
    // Do nothing
  }
  override async onMount(): Promise<void> {
    // Do nothing
  }

  override async beforeUnmount(): Promise<void> {
    this.getLogger()?.info(MultipleRedisService.name, "Disconnecting all Redis connections...");
    let keys = this.dbs.keys();
    for await (let key of keys) {
      let db = this.dbs.get(key);
      await db?.beforeUnmount();
      await db?.onUnmount();
      this.dbs.delete(key);
    }
    this.getLogger()?.info(MultipleRedisService.name, "Successfully disconnected from all Redis connections.");
  }

  override async onUnmount(): Promise<void> {
    // Do nothing
  }

  /** Create a Mysql Database Service and add in the list of active connections. It throws an ´CoreError´ if there is not an active connection with the given name. */
  async createConnection(name: string, params: RedisParamsInterface) {
    if (this.dbs.has(name)) {
      throw new TyapiError(`There is already a connection defined with the name '${name}'`);
    } else {
      let db = new RedisService(params, this.context);
      await db.beforeMount();
      await db.onMount();
      this.dbs.set(name, db);
      this.getLogger()?.info(MultipleRedisService.name, "New connection to the database was added in the pool.");
    }
  }

  /** Returns the connection with the given name. It throws an `CoreError` if there is not an active connection with the given name and there are not any parameters given to start a new connection. */
  async getConnection(name: string, params?: RedisParamsInterface) {
    if (this.dbs.has(name)) {
      return this.dbs.get(name)!;
    } else if (params != null) {
      this.createConnection(name, params);
      return this.dbs.get(name)!;
    } else {
      throw new TyapiError(`No connection with the name '${name}' was found, and no parameters to create a new connection was provided.`);
    }
  }

  /** Close a connection with a given name. */
  async closeConnection(name: string) {
    if (this.dbs.has(name)) {
      let db = this.dbs.get(name)!;
      await db.beforeUnmount();
      await db.onUnmount();
      this.dbs.delete(name);
      this.getLogger()?.info(MultipleRedisService.name, "A connection to the database was removed from the pool.");
    }
  }

  /** Return the names of all active connections. */
  getDatabases() {
    return this.params.keys();
  }
}
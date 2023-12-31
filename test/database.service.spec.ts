import { assert } from "chai"
import { Context, Logger, LoggerMode } from "tyapi-core"
import { RedisService } from "../src/db/database.service"

class ExtendedService extends RedisService {
  constructor(context: Context) {
    super({
      host: "localhost",
      port: 6379
    }, context);
    this.params.set("STATUS", "READY")
  }
  async connect() {
    await super.connect()
    this.params.set("STATUS", "CONNECTED")
  }
  async disconnect() {
    await super.disconnect()
    this.params.set("STATUS", "DISCONNECTED")
  }
  public getStatus() {
    return this.params.get("STATUS") as string
  }
}

describe("RedisService", async () => {
  it("should create a connection with Redis and handle query commands", async () => {
    let app = new Context()
    app.mountService("logger", new Logger(LoggerMode.Console, app))
    let service = new ExtendedService(app)
    assert.equal(service.getStatus(), "READY")
    await app.mountService("database", service)
    assert.equal(service.getStatus(), "CONNECTED")

    await service.query("set", "hello", "world!")
    let ans = await service.query("get", "hello")
    assert.equal(ans, "world!")

    await service.query("setJson", "hellomessage", {name: "demo", lastName: "test"})
    let hans = await service.query("getJson", "hellomessage")
    assert.equal(hans.name, "demo")
    assert.equal(hans.lastName, "test")

    await service.query("del", "hello")
    await service.query("del", "hellomessage")

    let del = await service.query("get", "hello")
    let hdel = await service.query("getJson", "hellomessage")
    assert.equal(del, null)
    assert.equal(hdel, null)

    await app.unmountServices()
    assert.equal(service.getStatus(), undefined)
  })
})
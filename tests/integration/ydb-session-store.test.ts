import dotenv from "dotenv";
import fs from "fs";
import { AnonymousAuthService, Driver, DropTableSettings } from "ydb-sdk";
import { SessionStore } from "telegraf";
import { Ydb } from "../../src";

dotenv.config({ path: ".env.test" });

const endpoint = process.env.YDB_ENDPOINT!;
const database = process.env.YDB_DATABASE!;
const certFile = process.env.YDB_SSL_ROOT_CERTIFICATES_FILE!;
const driver = new Driver({
  endpoint,
  database,
  authService: new AnonymousAuthService(),
  sslCredentials: {
    rootCertificates: fs.readFileSync(certFile),
  },
});

interface StoredSession {
  a: number;
  b: number;
  c: string;
}

describe("create table", () => {
  let sessionStore: SessionStore<StoredSession>;

  beforeAll(async () => {
    await driver.ready(3000);
  });

  afterAll(async () => {
    await driver.destroy();
  });

  beforeEach(async () => {
    await driver.tableClient.withSession(async (session) => {
      await session.dropTable(
        "telegraf-sessions",
        new DropTableSettings({ muteNonExistingTableErrors: true }),
      );
    });

    sessionStore = Ydb({
      driver,
      tableOptions: {
        shouldCreateTable: true,
      },
    });
  });

  test("should create table when get called", async () => {
    const session = await sessionStore.get("key");
    expect(session).toBeUndefined();

    const tableExists = await checkTableExists("telegraf-sessions");
    expect(tableExists).toBeTruthy();
  });

  test("should create table when set called", async () => {
    await sessionStore.set("key", { a: 1, b: 2, c: "3" });

    const tableExists = await checkTableExists("telegraf-sessions");
    expect(tableExists).toBeTruthy();
  });

  test("should create table when delete called", async () => {
    await sessionStore.delete("key");

    const tableExists = await checkTableExists("telegraf-sessions");
    expect(tableExists).toBeTruthy();
  });
});

async function checkTableExists(table: string) {
  try {
    await driver.tableClient.withSession(async (session) => {
      await session.executeQuery(`SELECT 1 FROM \`${table}\` LIMIT 1`);
    });
    return true;
  } catch (e) {
    return false;
  }
}
